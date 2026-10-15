/*
 * File:
 *    pth_pool.c
 *
 * Purpose:
 *    Implementacao do modelo Produtor/Consumidor utilizando
 *    um pool de threads, fila compartilhada e variaveis
 *    de condicao.
 *
 * Compilacao:
 *    gcc -g -Wall -Wextra -o pth_pool pth_pool.c -pthread
 *
 * Execucao - cenario de fila cheia:
 *    ./pth_pool cheia
 *
 * Execucao - cenario de fila vazia:
 *    ./pth_pool vazia
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <stdint.h>
#include <string.h>

#define PRODUCER_NUM 3
#define CONSUMER_NUM 3

#define BUFFER_SIZE 3
#define TASKS_PER_PRODUCER 5

/*
 * Cenario cheia:
 *   produtoras: 1 segundo
 *   consumidoras: 2 segundos
 *
 * Cenario vazia:
 *   produtoras: 2 segundos
 *   consumidoras: 1 segundo
 */
int producer_delay;
int consumer_delay;

/*
 * producer_id identifica a thread que gerou 
 * sequence identifica qual relogio 
 * stop indica uma mensagem para encerrar 
 */
typedef struct Task {
    int clock[3];
    int producer_id;
    int sequence;
    int stop;
} Task;

Task taskQueue[BUFFER_SIZE];

int taskCount = 0;
int queueHead = 0;
int queueTail = 0;

pthread_mutex_t mutex;

/*
 * condFull:
 *   usada pelas produtoras quando a fila esta cheia
 *
 * condEmpty:
 *   usada pelas consumidoras quando a fila esta vazia
 */
pthread_cond_t condFull;
pthread_cond_t condEmpty;


/*
 * Insere uma tarefa na fila
 *
 * Espera se a fila estiver cheia
 */
void submitTask(Task task) {

    pthread_mutex_lock(&mutex);

    while (taskCount == BUFFER_SIZE) {
        printf("[Fila cheia] Produtora aguardando espaco.\n");
        fflush(stdout);

        pthread_cond_wait(&condFull, &mutex);
    }

    /* Insere no final da fila circular */
    taskQueue[queueTail] = task;
    queueTail = (queueTail + 1) % BUFFER_SIZE;
    taskCount++;

    printf("[Fila] Item inserido. Ocupacao: %d/%d\n",
           taskCount, BUFFER_SIZE);
    fflush(stdout);

    /*
     * A fila deixou de estar vazia
     */
    pthread_cond_signal(&condEmpty);

    pthread_mutex_unlock(&mutex);
}


/*
 * Retira uma tarefa da fila
 *
 * Espera se a fila estiver vazia
 */
Task getTask(void) {

    pthread_mutex_lock(&mutex);

    while (taskCount == 0) {
        printf("[Fila vazia] Consumidora aguardando item.\n");
        fflush(stdout);

        pthread_cond_wait(&condEmpty, &mutex);
    }

    /* Retira o item mais antigo da fila */
    Task task = taskQueue[queueHead];
    queueHead = (queueHead + 1) % BUFFER_SIZE;
    taskCount--;

    printf("[Fila] Item removido. Ocupacao: %d/%d\n",
           taskCount, BUFFER_SIZE);
    fflush(stdout);

    /*
     * A fila deixou de estar cheia
     */
    pthread_cond_signal(&condFull);

    pthread_mutex_unlock(&mutex);

    return task;
}

void executeTask(Task *task, int consumer_id) {

    printf("[Consumidora %d] Relogio produzido por P%d"
           " (item %d): (%d,%d,%d)\n",
           consumer_id,
           task->producer_id,
           task->sequence,
           task->clock[0],
           task->clock[1],
           task->clock[2]);

    fflush(stdout);
}


/*
 * Cada produtora possui seu proprio relogio local
 * A cada item produzido, incrementa
 */
void *startProducer(void *args) {

    int id = (int)(intptr_t)args;

    int clock[3] = {0, 0, 0};

    for (int i = 1; i <= TASKS_PER_PRODUCER; i++) {

        clock[id]++;

        Task task = {
            .clock = {clock[0], clock[1], clock[2]},
            .producer_id = id,
            .sequence = i,
            .stop = 0
        };

        printf("[Produtora %d] Gerou relogio %d: (%d,%d,%d)\n",
               id, i, clock[0], clock[1], clock[2]);
        fflush(stdout);

        /* Coloca o relogio na fila compartilhada */
        submitTask(task);

        /* Controla a taxa de producao */
        sleep((unsigned int)producer_delay);
    }

    printf("[Produtora %d] Finalizou a producao.\n", id);
    fflush(stdout);

    return NULL;
}


/*
 * Cada consumidora retira itens ate receber uma tarefa
 * especial de encerramento (stop == 1).
 */
void *startConsumer(void *args) {

    int id = (int)(intptr_t)args;

    while (1) {

        Task task = getTask();

        /*
         * A tarefa especial encerra a thread
         */
        if (task.stop) {
            break;
        }

        executeTask(&task, id);

        /* Controla a taxa de consumo */
        sleep((unsigned int)consumer_delay);
    }

    printf("[Consumidora %d] Finalizou.\n", id);
    fflush(stdout);

    return NULL;
}


int main(int argc, char *argv[]) {

    if (argc != 2 ||
        (strcmp(argv[1], "cheia") != 0 &&
         strcmp(argv[1], "vazia") != 0)) {

        fprintf(stderr,
                "Uso: %s {cheia|vazia}\n",
                argv[0]);

        return EXIT_FAILURE;
    }

    /*
     * Seleciona os intervalos de producao e consumo
     */
    if (strcmp(argv[1], "cheia") == 0) {
        producer_delay = 1;
        consumer_delay = 2;
    } else {
        producer_delay = 2;
        consumer_delay = 1;
    }

    printf("Cenario: %s\n", argv[1]);
    printf("Produtoras: %d | Consumidoras: %d\n",
           PRODUCER_NUM, CONSUMER_NUM);
    printf("Intervalo de producao: %d s\n", producer_delay);
    printf("Intervalo de consumo: %d s\n", consumer_delay);
    printf("Capacidade da fila: %d\n\n", BUFFER_SIZE);

    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&condEmpty, NULL);
    pthread_cond_init(&condFull, NULL);

    pthread_t producers[PRODUCER_NUM];
    pthread_t consumers[CONSUMER_NUM];

    /*
     * Primeiro criamos as consumidoras
     */
    for (intptr_t i = 0; i < CONSUMER_NUM; i++) {
        if (pthread_create(&consumers[i], NULL,
                           startConsumer, (void *)i) != 0) {
            perror("Erro ao criar consumidora");
            exit(EXIT_FAILURE);
        }
    }

    /* Cria as tres produtoras */
    for (intptr_t i = 0; i < PRODUCER_NUM; i++) {
        if (pthread_create(&producers[i], NULL,
                           startProducer, (void *)i) != 0) {
            perror("Erro ao criar produtora");
            exit(EXIT_FAILURE);
        }
    }

    /*
     * Aguarda todas as produtoras terminarem
     */
    for (int i = 0; i < PRODUCER_NUM; i++) {
        pthread_join(producers[i], NULL);
    }

    /*
     * Todas as produtoras terminaram. Inserimos uma tarefa
     * especial para cada consumidora poder encerrar
     *
     * Se a fila estiver cheia, submitTask aguarda ate
     * que uma consumidora libere espaco
     */
    for (int i = 0; i < CONSUMER_NUM; i++) {

        Task stopTask = {
            .clock = {0, 0, 0},
            .producer_id = -1,
            .sequence = 0,
            .stop = 1
        };

        submitTask(stopTask);
    }

    /* Aguarda todas as consumidoras terminarem */
    for (int i = 0; i < CONSUMER_NUM; i++) {
        pthread_join(consumers[i], NULL);
    }

    /* Libera os recursos */
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&condEmpty);
    pthread_cond_destroy(&condFull);

    printf("\nPrograma finalizado com sucesso.\n");

    return EXIT_SUCCESS;
}
