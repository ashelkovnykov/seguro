//! @file fdb.c
//!
//! Reads and writes events into and out of a FoundationDB cluster.

#include <foundationdb/fdb_c.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>

#include "constants.h"
#include "fdb.h"

#define CLEAR_BATCH_SIZE (40000 * fdb_batch_size)
#define KEY_TOTAL_LENGTH (1 + KEY_EVENT_LENGTH + KEY_FRAGMENT_LENGTH)
#define KEY_EVENT_LENGTH 8
#define KEY_FRAGMENT_LENGTH 4


//==============================================================================
// Variables
//==============================================================================

pthread_t fdb_network_thread;
uint32_t  fdb_batch_size = 1;

//==============================================================================
// Prototypes
//==============================================================================

uint32_t add_event_set_transactions(FDBTransaction *tx, FragmentedEvent *event, uint32_t start_pos, uint32_t limit);

void add_event_clear_transaction(FDBTransaction *tx, FragmentedEvent *event);

uint8_t count_digits(uint64_t n);

//==============================================================================
// Functions
//==============================================================================

int fdb_set_batch_size(uint32_t batch_size) {
  if (!batch_size) return -1;

  fdb_batch_size = batch_size;
  return 0;
}

void *fdb_init_run_network(void *arg) {

  printf("Starting network thread...\n\n");

  fdb_error_t err = fdb_run_network();
  if (err != 0) {
    printf("fdb_init_run_network: %s\n", fdb_get_error(err));
    exit(-1);
  }

  return NULL;
}

FDBDatabase *fdb_init(void) {

  const char *cluster_file_path = "/etc/foundationdb/fdb.cluster";
  FDBFuture *fdb_future = NULL;
  
  // Check cluster file attributes, exit if not found
  struct stat cluster_file_buffer;
  uint32_t cluster_file_stat = stat(cluster_file_path, &cluster_file_buffer);
  if (cluster_file_stat != 0) {
    printf("ERROR: no fdb.cluster file found at: %s\n", cluster_file_path);
    return NULL;
  }
  
  // Ensure correct FDB API version
  printf("Ensuring correct API version...\n");
  fdb_error_t err;
  err = fdb_select_api_version(FDB_API_VERSION);
  if (err != 0) {
    printf("fdb_init select api version: %s\n", fdb_get_error(err));
    goto fdb_init_error;
  }

  // Setup FDB network
  printf("Setting up network...\n");
  err = fdb_setup_network();
  if (err != 0) {
    printf("fdb_init failed to setup fdb network: %s\n", fdb_get_error(err));
    goto fdb_init_error;
  }

  // Start the network thread
  pthread_create(&fdb_network_thread, NULL, fdb_init_run_network, NULL);

  // Create the database
  printf("Creating the database...\n");
  FDBDatabase *fdb;
  err = fdb_create_database((char *) cluster_file_path, &fdb);
  if (err != 0) {
    printf("fdb_init create database: %s\n", fdb_get_error(err));
    goto fdb_init_error;
  }

  // Success
  return fdb;

  // FDB initialization error goto.
  fdb_init_error:
  if (fdb_future) {
    fdb_future_destroy(fdb_future);
  }

  // Failure
  return NULL;
}

int fdb_shutdown(FDBDatabase *fdb, pthread_t *t) {

  fdb_error_t err;

  // Destroy the database
  fdb_database_destroy(fdb);

  // Signal network shutdown
  err = fdb_stop_network();
  if (err != 0) {
    printf("fdb_init stop network: %s\n", fdb_get_error(err));
    return -1;
  }

  // Stop the network thread
  err = pthread_join(*t, NULL);
  if (err) {
    printf("fdb_init wait for network thread: %d\n", err);
    return -1;
  }

  // Success
  return 0;
}

int fdb_setup_transaction(FDBDatabase *fdb, FDBTransaction **tx) {

  // Create a new database "transaction" (actually a snapshot of diffs to apply as a single transaction)
  fdb_error_t err = fdb_database_create_transaction(fdb, tx);

  // Failure
  if (err != 0) {
    printf("fdb_database_create_transaction error:\n%s", fdb_get_error(err));
    return -1;
  }

  // Success
  return 0;
}

int fdb_send_transaction(FDBDatabase *fdb, FDBTransaction *tx) {

  fdb_error_t err;

  // Commit event batch transaction
  FDBFuture *future = fdb_transaction_commit(tx);

  // TODO: Synchornous; need to test asynchronous version
  // Wait for the future to be ready
  err = fdb_future_block_until_ready(future);
  if (err != 0) {
    printf("fdb_future_block_until_ready error:\n%s", fdb_get_error(err));
    return -1;
  }

  // Check that the future did not return any errors
  err = fdb_future_get_error(future);
  if (err != 0) {
    printf("fdb_future_error:\n%s\n", fdb_get_error(err));
    return -1;
  }

  // Destroy the future
  fdb_future_destroy(future);

  //
  fdb_transaction_reset(tx);

  // SUCCESS
  return 0;
}

int fdb_write_batch(FDBDatabase *fdb, FragmentedEvent *event, uint32_t *pos) {

  FDBTransaction *tx;
  int             err;

  err = fdb_setup_transaction(fdb, &tx);
  if (err) return err;

  *pos += add_event_set_transactions(tx, event, *pos, fdb_batch_size);

  err = fdb_send_transaction(fdb, tx);
  if (err) return err;

  fdb_transaction_destroy(tx);

  return 0;
}

int fdb_write_event(FDBDatabase *fdb, FragmentedEvent *event) {

  FDBTransaction *tx;
  uint32_t        i = 0;
  int             err;

  err = fdb_setup_transaction(fdb, &tx);
  if (err) return err;

  while (i < event->num_fragments) {
    i += add_event_set_transactions(tx, event, i, fdb_batch_size);

    err = fdb_send_transaction(fdb, tx);
    if (err) return err;
  }

  fdb_transaction_destroy(tx);

  return 0;
}

int fdb_write_event_array(FDBDatabase *fdb, FragmentedEvent *events, uint32_t num_events) {

  FDBTransaction *tx;
  uint32_t        batch_filled = 0;
  uint32_t        frag_pos = 0;
  uint32_t        i = 0;
  int             err;

  err = fdb_setup_transaction(fdb, &tx);
  if (err) return err;

  while (i < num_events) {

    if (!batch_filled) {
      batch_filled = add_event_set_transactions(tx, (events + i), frag_pos, fdb_batch_size);
      frag_pos += batch_filled;
    } else {
      uint32_t num_kvp = add_event_set_transactions(tx, (events + i), frag_pos, (fdb_batch_size - batch_filled));
      batch_filled += num_kvp;
      frag_pos += num_kvp;
    }

    if (frag_pos == events[i].num_fragments) {
      i += 1;
      frag_pos = 0;
    }

    if (batch_filled == fdb_batch_size) {
      err = fdb_send_transaction(fdb, tx);
      if (err) return err;

      batch_filled = 0;
    }
  }

  fdb_send_transaction(fdb, tx);
  if (err) return err;

  fdb_transaction_destroy(tx);

  // Success
  return 0;
}

int fdb_clear_event(FDBDatabase *fdb, FragmentedEvent *event) {

  FDBTransaction *tx;
  int             err;

  err = fdb_setup_transaction(fdb, &tx);
  if (err) return err;

  add_event_clear_transaction(tx, event);

  fdb_send_transaction(fdb, tx);
  if (err) return err;

  fdb_transaction_destroy(tx);

  // Success
  return 0;
}

int fdb_clear_event_array(FDBDatabase *fdb, FragmentedEvent *events, uint32_t num_events) {

  FDBTransaction *tx;
  int             err;

  err = fdb_setup_transaction(fdb, &tx);
  if (err) return err;

  for (uint32_t i = 0; i < num_events; ++i) {

    add_event_clear_transaction(tx, (events + i));

    if (!(i % CLEAR_BATCH_SIZE)) {
      err = fdb_send_transaction(fdb, tx);
      if (err) return err;
    }
  }

  err = fdb_send_transaction(fdb, tx);
  if (err) return err;

  fdb_transaction_destroy(tx);

  return 0;
}

uint32_t add_event_set_transactions(FDBTransaction *tx, FragmentedEvent *event, uint32_t start_pos, uint32_t limit) {

  uint32_t max_pos = (start_pos + limit);
  uint32_t end_pos = (max_pos < event->num_fragments) ? max_pos : event->num_fragments;
  uint32_t num_kvp = end_pos - start_pos;
  uint8_t  key_digits = count_digits(event->key);

  for (uint32_t i = start_pos; i < end_pos; ++i) {

    uint8_t key_length = key_digits + count_digits(i) + 1;
    uint8_t key[key_length];
    sprintf((char *)key, "%ld:%d", event->key, i);

    if (end_pos == max_pos) {
      fdb_transaction_set(tx,
                          key,
                          key_length,
                          event->fragments[i],
                          event->final_frag_length);
    } else {
      fdb_transaction_set(tx,
                          key,
                          key_length,
                          event->fragments[i],
                          OPTIMAL_VALUE_SIZE);
    }
  }

  return num_kvp;
}

void add_event_clear_transaction(FDBTransaction *tx, FragmentedEvent *event) {

  uint8_t key_digits = count_digits(event->key);
  uint8_t start_key_length = key_digits + 2;
  uint8_t end_key_length = key_digits + count_digits(event->num_fragments - 1) + 1;
  uint8_t start_key[start_key_length];
  uint8_t end_key[end_key_length];

  sprintf((char *)start_key, "%ld:0",  event->key);
  sprintf((char *)end_key,   "%ld:%d", event->key, (event->num_fragments - 1));

  fdb_transaction_clear_range(tx,
                              start_key,
                              start_key_length,
                              end_key,
                              end_key_length);
}

uint8_t count_digits(uint64_t n) {

  if (n == 0) {
    return 1;
  }

  uint8_t count = 0;
  while (n != 0) {
    n = n / 10;
    ++count;
  }

  return count;
}
