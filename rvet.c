/**
 * Implementação de Relógios Vetoriais utilizando MPI
 *
 * Compilação:
 *     mpicc -o rvet rvet.c
 *
 * Execução:
 *     mpiexec -n 3 --oversubscribe ./rvet
 */

#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

/*
 * Estrutura do relógio vetorial.
 */
typedef struct Clock {
    int p[3];
} Clock;


/*
 * Imprime o estado atual do relógio vetorial.
 * 'acao' descreve o evento que acabou de ocorrer.
 * O rank identifica qual processo realizou a operação.
 */
void PrintClock(const char *acao, Clock *clock) {

    int pid;

    MPI_Comm_rank(MPI_COMM_WORLD, &pid);

    printf("[Atualizado por P%d] %s -> Clock = (%d,%d,%d)\n",
           pid,
           acao,
           clock->p[0],
           clock->p[1],
           clock->p[2]);

    fflush(stdout);
}


/*
 * Evento interno.
 */
void Event(Clock *clock) {

    int pid;

    MPI_Comm_rank(MPI_COMM_WORLD, &pid);

    clock->p[pid]++;

    PrintClock("Evento interno", clock);
}


/*
 * Envio de mensagem contendo o relógio vetorial.
 *
 * Parâmetros:
 *     dest  -> rank do processo destinatário
 *     clock -> relógio local do processo remetente
 *
 * Funcionamento:
 *     1. Incrementa o relógio local.
 *     2. Envia o vetor atualizado.
 *     3. Imprime o estado do relógio.
 */
void Send(int dest, Clock *clock) {

    int pid;

    MPI_Comm_rank(MPI_COMM_WORLD, &pid);

    /*
     * Todo envio é considerado um evento interno.
     */
    clock->p[pid]++;

    /*
     * Envia as três posições do relógio vetorial.
     *
     * MPI_INT -> tipo dos elementos
     * 3       -> quantidade de elementos
     * dest    -> processo destinatário
     * 0       -> identificador da mensagem
     */
    MPI_Send(clock->p,
             3,
             MPI_INT,
             dest,
             0,
             MPI_COMM_WORLD);

    PrintClock("Envio de mensagem", clock);
}


/*
 * Recebimento de mensagem contendo um relógio vetorial.
 *
 * Parâmetros:
 *     src   -> rank do processo remetente
 *     clock -> relógio local do receptor
 *
 * Funcionamento:
 *     1. Recebe o vetor remoto.
 *     2. Faz a fusão usando o maior valor de cada posição.
 *     3. Incrementa o componente local.
 *     4. Imprime o relógio atualizado.
 */
void Receive(int src, Clock *clock) {

    int pid;

    MPI_Comm_rank(MPI_COMM_WORLD, &pid);

    /*
     * Armazena temporariamente o relógio recebido.
     */
    int received_clock[3];

    /*
     * Recebe o vetor enviado pelo processo.
     */
    MPI_Recv(received_clock,
             3,
             MPI_INT,
             src,
             0,
             MPI_COMM_WORLD,
             MPI_STATUS_IGNORE);

    /*
     * Fusão dos relógios:
     *
     * Para cada posição, conserva o maior valor
     * entre o relógio local e o relógio recebido.
     */
    for (int i = 0; i < 3; i++) {

        if (received_clock[i] > clock->p[i]) {
            clock->p[i] = received_clock[i];
        }
    }

    /*
     * O recebimento é um evento interno.
     */
    clock->p[pid]++;

    PrintClock("Recebimento de mensagem", clock);
}


/*
 * Sequência de operações do Processo 0:
 * EVENT()
 * SEND(1)
 * RECEIVE()
 * SEND(2)
 * RECEIVE()
 * SEND(1)
 * EVENT()
 */
void process0() {

    Clock clock = {{0, 0, 0}};

    PrintClock("Estado inicial", &clock);

    Event(&clock);

    Send(1, &clock);

    Receive(1, &clock);

    Send(2, &clock);

    Receive(2, &clock);

    Send(1, &clock);

    Event(&clock);
}


/*
 * Sequência de operações do Processo 1:
 * SEND(0)
 * RECEIVE()
 * RECEIVE()
 */
void process1() {

    Clock clock = {{0, 0, 0}};

    PrintClock("Estado inicial", &clock);

    Send(0, &clock);

    Receive(0, &clock);

    Receive(0, &clock);
}


/*
 * Sequência de operações do Processo 2:
 * EVENT()
 * SEND(0)
 * RECEIVE()
 */
void process2() {

    Clock clock = {{0, 0, 0}};

    PrintClock("Estado inicial", &clock);

    Event(&clock);

    Send(0, &clock);

    Receive(0, &clock);
}


int main(void) {

    int my_rank;
    int comm_size;

    MPI_Init(NULL, NULL);

    /*
     * Obtém o rank do processo atual.
     */
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    /*
     * Obtém a quantidade total de processos.
     */
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);

    if (comm_size != 3) {

        if (my_rank == 0) {
            fprintf(stderr,
                    "Erro: execute o programa com exatamente 3 processos.\n");
        }

        MPI_Finalize();

        return EXIT_FAILURE;
    }

    /*
     * Cada processo executa sua própria sequência.
     */
    if (my_rank == 0) {

        process0();

    } else if (my_rank == 1) {

        process1();

    } else if (my_rank == 2) {

        process2();
    }

    /*
     * Finaliza o ambiente MPI.
     */
    MPI_Finalize();

    return EXIT_SUCCESS;
}