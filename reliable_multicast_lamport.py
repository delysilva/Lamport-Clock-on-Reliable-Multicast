import threading
import time
import random
import queue
from datetime import datetime

# Número de processos na simulação
NUM_PROCESSOS = 6

class Processo(threading.Thread):
    """
    Representa um processo no sistema distribuído.
    Cada processo é uma thread independente.
    """
    def __init__(self, process_id: int, all_queues: list):
        super().__init__()
        self.id = process_id
        self.all_queues = all_queues
        self.message_queue = self.all_queues[self.id]
        
        # Estruturas de dados do processo
        self.lamport_clock = 0
        self.delivered_messages = set() # Para garantir a entrega única (evita loops de gossip)
        self.lock = threading.Lock() # Para acesso seguro ao relógio e ao conjunto de mensagens
        self.running = True

    def _tick(self):
        """Incrementa o relógio de Lamport (evento interno)."""
        self.lamport_clock += 1

    def handle_receive(self, received_message: dict):
        """
        Processa uma mensagem recebida. Implementa a Regra 3 de Lamport e o
        protocolo de multicast confiável (gossip).
        """
        original_sender = received_message['original_sender']
        timestamp = received_message['timestamp']
        content = received_message['content']
        
        # Uma mensagem é única pela combinação de seu remetente original e seu timestamp
        message_unique_id = (original_sender, timestamp)

        with self.lock:
            # Se já entregamos esta mensagem, ignoramos para evitar loops
            if message_unique_id in self.delivered_messages:
                return

            # Atualiza o relógio de Lamport (Regra 3)
            self.lamport_clock = max(self.lamport_clock, timestamp) + 1
            
            # Marca a mensagem como entregue
            self.delivered_messages.add(message_unique_id)

            # Exibe a entrega da mensagem de forma clara
            print(
                f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] "
                f"Processo {self.id}: MENSAGEM ENTREGUE! "
                f"Conteúdo: '{content}' | "
                f"Remetente Original: {original_sender} | "
                f"Timestamp da Mensagem: {timestamp} | "
                f"Meu Relógio Agora: {self.lamport_clock}", flush=True
            )

        # Retransmite a mensagem para todos os outros para garantir a confiabilidade (Gossip)
        self._basic_multicast(received_message)

    def multicast(self, message_content: str):
        """
        Inicia o envio de uma mensagem multicast para todos os outros processos.
        """
        with self.lock:
            # Incrementa o relógio para o evento de envio (Regra 1 de Lamport)
            self._tick()
            
            print(
                f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] "
                f"Processo {self.id}: ENVIANDO MULTICAST... "
                f"Conteúdo: '{message_content}' | "
                f"Meu Relógio: {self.lamport_clock}", flush=True
            )
            
            message = {
                'content': message_content,
                'timestamp': self.lamport_clock,
                'original_sender': self.id
            }

        # Envia a mensagem para todos os processos (incluindo ele mesmo, por simplicidade,
        # embora pudesse ser otimizado para não enviar a si mesmo)
        self._basic_multicast(message)

    def _basic_multicast(self, message: dict):
        """
        Função auxiliar que envia a mensagem para as filas de todos os processos.
        """
        for i in range(len(self.all_queues)):
            self.all_queues[i].put(message)

    def run(self):
        """
        Loop principal do processo. Fica escutando por mensagens em sua fila.
        """
        while self.running:
            try:
                # Espera por uma mensagem na fila, com um timeout para poder parar a thread
                message = self.message_queue.get(timeout=0.1)
                
                # Mensagem especial para parar a thread
                if message.get('type') == 'STOP':
                    self.running = False
                    continue

                self.handle_receive(message)

            except queue.Empty:
                # Nenhuma mensagem na fila, continua o loop
                continue

    def stop(self):
        """Sinaliza para a thread parar."""
        self.all_queues[self.id].put({'type': 'STOP'})


if __name__ == "__main__":
    print("--- Iniciando Simulação de Multicast Confiável com Relógios de Lamport ---")
    print(f"--- Simulação com {NUM_PROCESSOS} processos ativos ---")

    # Cria uma fila para cada processo
    filas_de_mensagens = [queue.Queue() for _ in range(NUM_PROCESSOS)]

    # Cria e inicia os processos
    processos = [Processo(i, filas_de_mensagens) for i in range(NUM_PROCESSOS)]
    
    for p in processos:
        p.start()

    # Dá um tempo para as threads inicializarem
    time.sleep(1)

    # --- Simulação de Eventos ---
    # Processos diferentes enviam mensagens em momentos diferentes.
    
    print("\n--- Disparando Eventos de Multicast ---\n")
    
    # Evento 1: Processo 2 envia uma mensagem
    time.sleep(random.uniform(0, 1))
    processos[2].multicast("Primeira mensagem da simulação")
    
    # Evento 2: Processo 5 envia uma mensagem
    time.sleep(random.uniform(0, 1.5))
    processos[5].multicast("Saudações do processo 5")

    # Evento 3: Processo 0 envia uma mensagem
    time.sleep(random.uniform(0, 1))
    processos[0].multicast("Esta mensagem deve ter um timestamp maior")

    # Espera um tempo suficiente para que todas as mensagens se propaguem e sejam entregues
    print("\n--- Aguardando propagação das mensagens (10 segundos) ---\n")
    time.sleep(10)

    # Para todas as threads
    print("--- Encerrando a simulação e parando os processos... ---")
    for p in processos:
        p.stop()
    
    for p in processos:
        p.join() # Espera a thread realmente terminar

    print("--- Simulação Concluída ---")