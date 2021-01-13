#if !defined(EXTERNAL_SORT_H)
#define EXTERNAL_SORT_H

#include <stdint.h>
#include "file/ion_file.h"

#if defined(__cplusplus)
extern "C" {
#endif

typedef struct {
    uint16_t	key_size;
    uint16_t	value_size;
    uint16_t	page_size;
    uint16_t	record_size;
    uint32_t    num_pages;
    uint16_t    num_values_last_page;
    int8_t      headerSize;
    int8_t      (*compare_fcn)(void *a, void *b);
} external_sort_t;

typedef struct {
    uint32_t num_reads;
    uint32_t num_writes;
    uint32_t num_memcpys;
    uint32_t num_compar;
    uint32_t num_runs;
    double time;
    uint32_t genTime;
} metrics_t;

typedef struct {
    int32_t key;
    char	value[12];
} test_record_t;

typedef struct {
	ION_FILE *file;
	uint32_t recordsRead;
	uint32_t totalRecords;
	uint32_t recordSize;
} file_iterator_state_t;

/* Constant declarations */
#define    BLOCK_HEADER_SIZE    sizeof(int32_t)+sizeof(int16_t)
#define    BLOCK_ID_OFFSET      0
#define    BLOCK_COUNT_OFFSET   sizeof(uint32_t)


#if defined(__cplusplus)
}
#endif

#endif
