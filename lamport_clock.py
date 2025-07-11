import socket
import threading
import time
import json
import collections

# Configurações de Rede (simplificadas para localhost)
BASE_PORT = 12000 # Porta inicial para os processos
NUM_PROCESSES = 5 # Número total de processos na rede

# Tempo de espera para retransmissão
RETRANSMISSION_INTERVAL = 2 # segundos

class ReliableMulticastProcess:
    def __init__(self, process_id, total_processes):
        self.process_id = process_id
        self.total_processes = total_processes
        self.port = BASE_PORT + process_id
        self.addr = ('127.0.0.1', self.port)

        self.lamport_clock = 0 # Relógio de Lamport inicial
        self.delivered_messages = set() # Mensagens entregues localmente (usado para Integridade)
        
        # pending_acks: {message_id: {process_id_of_acks_pending}}
        # message_id é uma string formatada como "sender_id-original_lamport_timestamp-message_content"
        self.pending_acks = collections.defaultdict(set) 
        
        # buffer_messages: Guarda mensagens DATA que ainda não foram entregues/processadas
        # ou que esperam uma retransmissão de ACK caso o remetente não receba
        self.buffer_messages = {} # {message_id: {"content": msg_content, "sender_id": original_sender_id, "lamport": lamport_timestamp}}


        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(self.addr)
        self.socket.settimeout(0.1) # Timeout para o recvfrom para não bloquear threads

        print(f"Process P{self.process_id} started on port {self.port}. Initial Lamport Clock: {self.lamport_clock}")

        self.listener_thread = threading.Thread(target=self._listen_for_messages)
        self.listener_thread.daemon = True
        self.listener_thread.start()

        self.retransmitter_thread = threading.Thread(target=self._retransmit_messages)
        self.retransmitter_thread.daemon = True
        self.retransmitter_thread.start()
    
    def _increment_lamport_clock(self):
        self.lamport_clock += 1
        return self.lamport_clock

    def _update_lamport_clock(self, received_lamport_timestamp):
        self.lamport_clock = max(self.lamport_clock, received_lamport_timestamp) + 1
        return self.lamport_clock

    def _send_message(self, message_type, message_id, content, sender_id, lamport_timestamp, target_addr):
        message = {
            "type": message_type,
            "message_id": message_id, # Usado para identificar a mensagem para ACKs
            "content": content,
            "sender_id": sender_id,
            "lamport": lamport_timestamp
        }
        try:
            self.socket.sendto(json.dumps(message).encode('utf-8'), target_addr)
        except Exception as e:
            print(f"P{self.process_id}: Error sending message to {target_addr}: {e}")

    def r_multicast(self, message_content):
        # 1. Incrementa o relógio de Lamport antes de enviar
        current_lamport = self._increment_lamport_clock()
        
        # message_id: Usado para identificar unicamente a mensagem para ACKs e evitar duplicação
        # Combina sender_id, timestamp de Lamport e conteúdo para maior unicidade
        message_id = f"{self.process_id}-{current_lamport}-{message_content}"
        
        # O remetente espera ACKs de todos os outros processos
        # NOTA: O remetente não precisa de ACK de si mesmo para entregar,
        # mas adicionamos para simplificar a lógica de "todos confirmaram"
        # para a remoção da mensagem do `pending_acks`. 
        # No `_on_message_received`, o próprio processo irá `r_deliver` e enviar um ACK para si mesmo.
        self.pending_acks[message_id] = set(range(self.total_processes))
        
        print(f"\nP{self.process_id} [Lamport: {self.lamport_clock}]: r_multicast(\"{message_content}\")")
        
        # Envia a mensagem para todos os processos
        for i in range(self.total_processes):
            target_addr = ('127.0.0.1', BASE_PORT + i)
            self._send_message("DATA", message_id, message_content, self.process_id, current_lamport, target_addr)
        
    def r_deliver(self, message_id, original_content, lamport_timestamp):
        # 3. Atualiza o relógio de Lamport ao entregar
        self._update_lamport_clock(lamport_timestamp)
        print(f"P{self.process_id} [Lamport: {self.lamport_clock}]: r_deliver(\"{original_content}\") (ID: {message_id}, Original Lamport: {lamport_timestamp})")

    def _listen_for_messages(self):
        while True:
            try:
                data, addr = self.socket.recvfrom(1024)
                received_message = json.loads(data.decode('utf-8'))
                
                msg_type = received_message["type"]
                message_id = received_message["message_id"]
                msg_content = received_message["content"]
                original_sender_id = received_message["sender_id"]
                original_lamport = received_message["lamport"]
                
                if msg_type == "DATA":
                    self._on_message_received(message_id, msg_content, original_sender_id, original_lamport, addr)
                elif msg_type == "ACK":
                    self._on_ack_received(message_id, original_sender_id) # original_sender_id do ACK é o ID de quem enviou o ACK
            except socket.timeout:
                pass # Não há mensagens, continua o loop
            except Exception as e:
                # Tratamento de erro básico para evitar que a thread pare
                # print(f"P{self.process_id} Error listening: {e}") 
                pass

    def _on_message_received(self, message_id, msg_content, original_sender_id, original_lamport, sender_addr):
        # 2. Atualiza o relógio de Lamport ao receber
        self._update_lamport_clock(original_lamport)
        
        print(f"P{self.process_id} [Lamport: {self.lamport_clock}]: Received DATA {message_id} from P{original_sender_id} (via {sender_addr[1]})")

        # Integridade: entrega no máximo uma vez
        if message_id not in self.delivered_messages:
            self.delivered_messages.add(message_id)
            self.buffer_messages[message_id] = {
                "content": msg_content, 
                "sender_id": original_sender_id, 
                "lamport": original_lamport
            }
            
            # Aqui, para simplificar e demonstrar o Lamport, entregamos a mensagem imediatamente
            # Em um protocolo de consenso mais robusto, poderia haver uma fila de entrega ordenada.
            self.r_deliver(message_id, msg_content, original_lamport)
            
            # Envia ACK para o remetente original da mensagem DATA
            original_sender_target_addr = ('127.0.0.1', BASE_PORT + original_sender_id)
            self._send_message("ACK", message_id, "", self.process_id, self.lamport_clock, original_sender_target_addr)
            print(f"P{self.process_id} [Lamport: {self.lamport_clock}]: Sent ACK for {message_id} to P{original_sender_id}")
        else:
            # Mensagem já foi entregue, mas reenvia ACK caso o original tenha sido perdido
            original_sender_target_addr = ('127.0.0.1', BASE_PORT + original_sender_id)
            self._send_message("ACK", message_id, "", self.process_id, self.lamport_clock, original_sender_target_addr)
            # print(f"P{self.process_id} [Lamport: {self.lamport_clock}]: Re-sent ACK for {message_id} to P{original_sender_id} (already delivered)")

    def _on_ack_received(self, message_id, ack_sender_id):
        # Este método é executado *somente* pelo processo que fez r_multicast da mensagem
        # 2. Atualiza o relógio de Lamport ao receber ACK (um evento)
        self._increment_lamport_clock() 

        if message_id in self.pending_acks:
            if ack_sender_id in self.pending_acks[message_id]:
                self.pending_acks[message_id].remove(ack_sender_id)
                print(f"P{self.process_id} [Lamport: {self.lamport_clock}]: Received ACK for {message_id} from P{ack_sender_id}. Remaining ACKs: {len(self.pending_acks[message_id])}")

                if not self.pending_acks[message_id]:
                    print(f"P{self.process_id} [Lamport: {self.lamport_clock}]: All ACKs received for {message_id}. Message considered fully delivered by all correct processes.")
                    del self.pending_acks[message_id] # Limpa o estado da mensagem

    def _retransmit_messages(self):
        # Este thread é executado APENAS pelos processos que fizeram r_multicast
        while True:
            time.sleep(RETRANSMISSION_INTERVAL) # Intervalo de retransmissão
            # Itera sobre uma cópia da chave, pois o dicionário pode ser modificado por on_ack_received
            for message_id in list(self.pending_acks.keys()): 
                if self.pending_acks[message_id]: # Se ainda houver ACKs pendentes
                    # Extrai o ID do remetente original da message_id
                    original_sender_id_str, original_lamport_str, msg_content_part = message_id.split('-', 2)
                    original_sender_id = int(original_sender_id_str)
                    original_lamport = int(original_lamport_str)
                    
                    if original_sender_id == self.process_id: # Apenas retransmite as próprias mensagens
                        print(f"P{self.process_id} [Lamport: {self.lamport_clock}]: Retransmitting {message_id} to P{list(self.pending_acks[message_id])}")
                        for target_id in list(self.pending_acks[message_id]): # Itera sobre uma cópia
                            target_addr = ('127.0.0.1', BASE_PORT + target_id)
                            # Retransmite a mensagem DATA original
                            self._send_message("DATA", message_id, msg_content_part, original_sender_id, original_lamport, target_addr)

# --- Exemplo de Execução ---
if __name__ == "__main__":
    processes = []
    
    # Cria os processos
    print("Initializing processes...")
    for i in range(NUM_PROCESSES):
        processes.append(ReliableMulticastProcess(i, NUM_PROCESSES))
    
    time.sleep(1) # Dá um tempo para os sockets iniciarem e os threads rodarem

    print("\n--- Starting Multicast Events ---")

    # Processo 0 envia uma mensagem
    processes[0].r_multicast("Hello, everyone from P0!")
    time.sleep(0.5) # Pequeno atraso para demonstrar a assincronia

    # Processo 1 envia uma mensagem
    processes[1].r_multicast("Greetings from P1!")
    time.sleep(0.5)

    # Processo 2 envia uma mensagem
    processes[2].r_multicast("P2 says hi!")
    time.sleep(0.5)

    # Processo 0 envia outra mensagem
    processes[0].r_multicast("Another message from P0!")

    # Mantém os processos rodando por um tempo para demonstração e para garantir que todos os ACKs sejam processados
    print("\n--- All initial messages sent. Monitoring for ACKs and retransmissions... ---")
    try:
        while True:
            time.sleep(5) # Aguarda para ver os eventos e retransmissões
            # Opcional: imprimir o estado atual dos relógios periodicamente
            for p in processes:
                print(f"P{p.process_id} current Lamport Clock: {p.lamport_clock}")

    except KeyboardInterrupt:
        print("\nStopping processes...")
        # Em um cenário de produção, threads e sockets precisariam ser fechados de forma limpa.