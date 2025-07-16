# Reliable Multicast com Relógio de Lamport

Esta aplicação simula um protocolo de multicast confiável utilizando o algoritmo de gossip e ordena os eventos com relógios de Lamport.

## Requisitos

- Python 3.6 ou superior. Nenhuma biblioteca externa é necessária.

## Como Executar

1.  Certifique-se de ter o Python 3 instalado.
2.  Salve o código em um arquivo chamado `reliable_multicast_lamport.py`.
3.  Abra um terminal ou prompt de comando.
4.  Navegue até o diretório onde você salvou o arquivo.
5.  Execute o script com o seguinte comando:

    ```bash
    python reliable_multicast_lamport.py
    ```

## O que Esperar

O script iniciará uma simulação com um número pré-definido de processos (o padrão é 6). Ele simulará o envio de três mensagens multicast de processos diferentes em momentos escalonados.

O console exibirá em tempo real:
- O momento em que um processo envia uma mensagem multicast, junto com seu relógio lógico no momento do envio.
- O momento em que um processo **entrega** uma mensagem, mostrando o conteúdo, o remetente original, o timestamp da mensagem e o novo valor do relógio lógico do processo receptor após a atualização de Lamport.

A simulação aguarda 10 segundos para garantir que todas as mensagens se propaguem pelo sistema antes de encerrar.
