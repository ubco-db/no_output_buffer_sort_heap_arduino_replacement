/******************************************************************************/
/**
@file		test_no_output_buffer_sort_replace.c
@author		Riley Jackson, Ramon Lawrence
@brief		This file performance/correctness testing of no output sort heap.
@copyright	Copyright 2020
			The University of British Columbia,
			IonDB Project Contributors (see AUTHORS.md)
@par Redistribution and use in source and binary forms, with or without
	modification, are permitted provided that the following conditions are met:

@par 1.Redistributions of source code must retain the above copyright notice,
	this list of conditions and the following disclaimer.

@par 2.Redistributions in binary form must reproduce the above copyright notice,
	this list of conditions and the following disclaimer in the documentation
	and/or other materials provided with the distribution.

@par 3.Neither the name of the copyright holder nor the names of its contributors
	may be used to endorse or promote products derived from this software without
	specific prior written permission.

@par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
	AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
	IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
	ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
	LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
	CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
	SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
	INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
	CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
	ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
	POSSIBILITY OF SUCH DAMAGE.
*/
/******************************************************************************/
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "no_output_buffer_sort_replace.h"
#include "in_memory_sort.h"

#define EXTERNAL_SORT_MAX_RAND 1000000

/* Used to validate each individual input data item in the sorted output */
/*
#define DATA_COMPARE    1
*/

#ifdef DATA_COMPARE
int32_t sampleData[500];

int cmpfunc (const void * a, const void * b) {
   return ( *(int*)a - *(int*)b );
}
#endif

int seed;

/**
 * Generates random test data records
 */
int
external_sort_write_int32_random_data(
        ION_FILE *unsorted_file,
        int32_t num_values,
        uint16_t record_size)
{
    printf("Random Data: %lu\n", num_values);
    int8_t* buffer = (int8_t*) malloc(record_size);
  
    /* Data record is empty. Only need to reset to 0 once as reusing struct. */
    int32_t i;
    for (i = 0; i < record_size-sizeof(int32_t); i++)
    {
        buffer[i + sizeof(int32_t)] = 0;
    }

    for (i = 0; i < num_values; i++)
    {
        /* Generate random key */
        *((int32_t*)buffer) = rand() % EXTERNAL_SORT_MAX_RAND;
        #ifdef DATA_COMPARE 
        sampleData[i] = (int32_t) *((int32_t*)buffer);
        #endif
        if (0 == fwrite(buffer, (size_t)record_size, 1, unsorted_file))
        {
            free(buffer);
            return 10;
        }
    }
    #ifdef DATA_COMPARE
   	qsort(sampleData, (uint32_t) num_values, sizeof(uint32_t), cmpfunc);			    
    /*
    for (i = 0; i < num_values; i++) 
       printf("Key: %d\n", sampleData[i]);    
    */
    #endif    
    free(buffer);
    return 0;
}

/**
 * Generates increasing or decreasing test data records
 */
int
external_sort_write_int32_sequential_data(
        ION_FILE *unsorted_file,
        int32_t num_values,
        uint16_t record_size,
        int reverse)
{
    int8_t* buffer = (int8_t*) malloc((size_t)record_size);

    /* Data record is empty. Only need to reset to 0 once as reusing struct. */    
    int32_t i;
    for (i = 0; i < record_size-4; i++)
    {
        buffer[i + sizeof(int32_t)] = 0;
    }

    for (i = 0; i < num_values; i++)
    {
        if (reverse)
        {
            *((int32_t*)buffer) = num_values - i;
        }
        else
        {
            *((int32_t*)buffer) = i + 1;
        }

        if (0 == fwrite(buffer, (size_t)record_size, 1, unsorted_file))
        {
            free(buffer);
            return 10;
        }
    }
    free(buffer);
    return 0;
}

/**
 * Generates increasing or decreasing test data records, replaces approximate percentage of values with random values.
 */
int
external_sort_write_int32_sorted_updated_data(
        ION_FILE *unsorted_file,
        int32_t num_values,
        uint16_t record_size,
        int percent_random)
{
    int8_t* buffer = (int8_t*) malloc((size_t)record_size);
    
    /* Data record is empty. Only need to reset to 0 once as reusing struct. */
    int32_t i;
    for (i = 0; i < record_size-4; i++)
    {
        buffer[i + sizeof(int32_t)] = 0;
    }

    for (i = 0; i < num_values; i++)
    {
        if ((rand() % 100) < percent_random) *((int32_t*)buffer) = rand()%EXTERNAL_SORT_MAX_RAND;
        else *((int32_t*)buffer) = i + 1;

        if (0 == fwrite(buffer, (size_t)record_size, 1, unsorted_file))
        {
            free(buffer);
            return 10;
        }
    }
    free(buffer);
    return 0;
}

/**
 * Generates test data records
 */
int
external_sort_write_test_data(
        ION_FILE *unsorted_file,
        int32_t num_values,
        uint16_t record_size,
        int testDataType,
        external_sort_t *es,
        int percent_random,
        int numDistinct)
{
    printf("Data Type: %d  Percent random: %d  Num. distinct: %d\n", testDataType, percent_random, numDistinct);

    int8_t* buffer = (int8_t*) malloc((size_t)record_size);

    /* Data record is empty. Only need to reset to 0 once as reusing struct. */    
    uint32_t i, j;
    for (i = 0; i < record_size-4; i++)
    {
        buffer[i + sizeof(int32_t)] = 0;
    }

    int32_t blockIndex = 0;
    int recordKey = 0;
    int count = 0;
    uint16_t values_per_page = (es->page_size - es->headerSize) / es->record_size;

    for (i = 0; i < es->num_pages-1; i++)
    {
        for (j = 0; j < values_per_page; j++)
        {
            if (testDataType == 1)            
            {   // Reverse data
                *((int32_t*)buffer) = num_values - recordKey;
            }
            else if (testDataType == 0)
            {   // Sorted data
                *((int32_t*)buffer) = recordKey + 1;
            }
            else if (testDataType == 2)
            {   // Random data
                // *((int32_t*)buffer) = rand() % EXTERNAL_SORT_MAX_RAND;                
                *((int32_t*)buffer) = rand() % numDistinct; 
            }
            else if (testDataType == 3)
            {   // Percentage random data
                if ((rand() % 100) < percent_random) 
                    *((int32_t*)buffer) = rand()%EXTERNAL_SORT_MAX_RAND;
                else 
                    *((int32_t*)buffer) = recordKey + 1;
            }
            #ifdef DATA_COMPARE 
            sampleData[i] = (int32_t) *((int32_t*)buffer);
            #endif
            
            recordKey++;
            count++;
            // Write out record
            if (0 == fwrite(buffer, (size_t)record_size, 1, unsorted_file))
            {
                free(buffer);
                return 10;
            }
        }
        blockIndex++;
    }
   
    // Write out last page 
    uint16_t recordsLastPage = num_values - count;   
 

    for (j = 0; j < recordsLastPage; j++)
    {
        if (testDataType == 1)            
        {   // Reverse data
            *((int32_t*)buffer) = num_values - recordKey;
        }
        else if (testDataType == 0)
        {   // Sorted data
            *((int32_t*)buffer) = recordKey + 1;
        }
        else if (testDataType == 2)
        {   // Random data
           // *((int32_t*)buffer) = rand() % EXTERNAL_SORT_MAX_RAND;
            *((int32_t*)buffer) = rand() % numDistinct; 
        }
        else if (testDataType == 3)
        {   // Percentage random data
            if ((rand() % 100) < percent_random) 
                *((int32_t*)buffer) = rand()%EXTERNAL_SORT_MAX_RAND;
            else 
                *((int32_t*)buffer) = recordKey + 1;
        }
        #ifdef DATA_COMPARE 
        sampleData[i] = (int32_t) *((int32_t*)buffer);
        #endif
        recordKey++;
        // Write out record
        if (0 == fwrite(buffer, (size_t)record_size, 1, unsorted_file))
        {
            free(buffer);
            return 10;
        }
    }    

    free(buffer);

    #ifdef DATA_COMPARE
   	qsort(sampleData, (uint32_t) num_values, sizeof(uint32_t), cmpfunc);			    
    /*
    for (i = 0; i < num_values; i++) 
       printf("Key: %d\n", sampleData[i]);    
    */
    #endif  
    return 0;
}

/**
 * Iterates through records in a file returning NULL when no more records.
 */
int fileRecordIterator(void* state, void* buffer)
{
    file_iterator_state_t* fileState = (file_iterator_state_t*) state;

    if (fileState->recordsRead >= fileState->totalRecords)
        return 0;

    /* Read next record */
    /* TODO: Improve by reading a block at a time */
    fread(buffer, fileState->recordSize, 1, fileState->file);
    fileState->recordsRead++;
    return 1;
}

void runalltests_no_output_buffer_sort_block()
{
    int8_t          numRuns = 2;
    metrics_t       metric[numRuns];
    external_sort_t es;

    /* Set random seed */
    seed = time(0);  
    seed = 2020;  
    // seed = 1582823384;
    printf("Seed: %d\n", seed);
    srand(seed);       

    int mem;
    for(mem = 2; mem <= 2; mem++) 
    {
        printf("<---- New Tests M=%d ---->\n", mem);
        int t;
        for (t = 9; t < 10; t++) 
        {
            printf("--- Test Number %d ---\n", t);
            for (int r=0; r < numRuns; r++)
            {            
                printf("--- Run Number %d ---\n", (r+1));
                int buffer_max_pages = mem;
                    
                metric[r].num_reads = 0;
                metric[r].num_writes = 0;
                metric[r].num_compar = 0;
                metric[r].num_memcpys = 0;
                metric[r].num_runs = 0;

                es.key_size = sizeof(int32_t); 
                es.value_size = 12;
                es.headerSize = BLOCK_HEADER_SIZE ;
                es.record_size = es.key_size + es.value_size;
                es.page_size = 512;

                int32_t values_per_page = (es.page_size - es.headerSize) / es.record_size;
                int32_t num_test_values = values_per_page;
												   
                int k;
                for (k = 0; k < t; k++)
                {                  
                    num_test_values *= buffer_max_pages;                  
                }
                // num_test_values = values_per_page * 125;
                /* Add variable number of records so pages not completely full (optional) */
                // num_test_values += rand() % 10;
                es.num_pages = (uint32_t) (num_test_values + values_per_page - 1) / values_per_page; 
                es.compare_fcn = merge_sort_int32_comparator;

                /* Buffers and file offsets used by sorting algorithim*/
                long result_file_ptr;
                char *buffer = (char*) malloc((size_t) buffer_max_pages * es.page_size + es.record_size);
                char *tuple_buffer = buffer + es.page_size * buffer_max_pages;
                if (NULL == buffer) {
                    printf("Error: Out of memory!\n");
                    return;
                }

                /* Create the file and fill it with test data */
                ION_FILE *fp;
                fp = fopen("myfile7.bin", "w+b");
                if (NULL == fp) {
                    printf("Error: Can't open file!\n");
                    return;
                }

                // external_sort_write_int32_sequential_data(fp, num_test_values, es.record_size, 1);
                // external_sort_write_int32_random_data(fp, num_test_values, es.record_size);
                // external_sort_write_int32_sorted_updated_data(fp, num_test_values, es.record_size, 10);
                external_sort_write_test_data(fp, num_test_values, es.record_size, 2, &es, 0, 64);

                fflush(fp);
                fseek(fp, 0, SEEK_SET);

                file_iterator_state_t iteratorState;
                iteratorState.file = fp;
                iteratorState.recordsRead = 0;
                iteratorState.totalRecords = num_test_values;
                iteratorState.recordSize = es.record_size;

                /* Open output file */
                ION_FILE *outFilePtr;

                outFilePtr = fopen("tmpsort7.bin", "w+b");

                if (NULL == outFilePtr)
                {
                    printf("Error: Can't open output file!\n");			
                }

                /* Run and time the algorithim */
                printf("num test values: %li\n", num_test_values);
                printf("blocks:%li\n", es.num_pages);
                #if defined(ARDUINO)
                unsigned long startMillis = millis(); /* initial start time */
                #else
                clock_t start = clock();
                #endif                    

                int8_t runGenOnly = 0;        
                int err = no_output_buffer_sort_replace(&fileRecordIterator, &iteratorState, tuple_buffer, outFilePtr, buffer, buffer_max_pages, &es, &result_file_ptr, &metric[r], merge_sort_int32_comparator, runGenOnly);

                if (8 == err) {
                    printf("Out of memory!\n");
                } else if (10 == err) {
                    printf("File Read Error!\n");
                } else if (9 == err) {
                    printf("File Write Error!\n");
                    result_file_ptr = 0;
                }

                #if defined(ARDUINO)
                unsigned long duration = millis() - startMillis; /* initial start time */

                // duration = duration / 1000;
                printf("Elapsed Time: %lu ms\n", duration);
                metric[r].time = duration;
                #else
                clock_t end = clock();
                printf("Elapsed Time: %0.6f s\n", ((double) (end - start)) / CLOCKS_PER_SEC);
                metric[r].time = ((double) (end - start)) / CLOCKS_PER_SEC;
                #endif

                /* Verify the data is sorted*/
                int sorted = 1;    
                fflush(outFilePtr);   
                fclose(outFilePtr);
                outFilePtr = fopen("tmpsort.bin", "r+b");
                fp = outFilePtr;
                char *rec_last = (char*) malloc(es.record_size);
                memcpy(rec_last, buffer + es.headerSize, es.record_size);

                printf("Starting file offset: %lu\n", result_file_ptr);
                fseek(fp, result_file_ptr, SEEK_SET);

                uint32_t i;
                test_record_t last, *buf;
                int32_t numvals = 0;

                /* Read blocks of output file to check if sorted */
                for (i=0; i < es.num_pages; i++)
                {
                    if (0 == fread(buffer, es.page_size, 1, outFilePtr))
                    {	printf("Failed to read block.\n");
                        sorted = 0;
                    }

                    /* Read records from file */
                    int count = *((int16_t*) (buffer+BLOCK_COUNT_OFFSET));
                    /* printf("Block: %d Count: %d\n", *((int16_t*) buffer), count); */
                    char* addr = &(buffer[0]);
           
                    for (int j=0; j < count; j++)
                    {	
                        buf = (test_record_t*) (buffer+es.headerSize+j*es.record_size);				
                        numvals++;
                        #ifdef DATA_COMPARE
                        if (sampleData[numvals-1] != buf->key)
                        {
                            printf("Num: %d ",numvals);   
                            printf(" \tExpected: %li",sampleData[numvals-1]);   
                            printf(" \tActual: %li\n",buf->key);   
                            sorted = 0;                     
                        }
                        #endif
                        
                        if (i > 0 && last.key > buf->key)
                        {
                            sorted = 0;
                            printf("VERIFICATION ERROR Offset: %li",ftell(outFilePtr)-es.page_size);
                            printf(" Block header: %d",*((int32_t*) addr));
                            printf(" Records: %d",*((int16_t*) (addr+BLOCK_COUNT_OFFSET)));
                            printf(" Record key: %li\n", ((test_record_t*) (addr+BLOCK_HEADER_SIZE))->key);
                            printf("%li not less than %li\n", last.key, buf->key);
                        }

                        memcpy(&last, buf, es.record_size);				
                    }
                    /* Need to preserve buf between page loads as buffer is repalced */
                }		

                if (numvals != num_test_values)
                {
                    printf("ERROR: Missing values: %d\n", (num_test_values-numvals));
                    sorted = 0;
                };

                /* Print Results*/
                printf("Sorted: %d\n", sorted);
                printf("Reads:%li\n", metric[r].num_reads);
                printf("Writes:%li\n", metric[r].num_writes);
                printf("I/Os:%li\n\n", metric[r].num_reads + metric[r].num_writes);
                printf("Num Comparisons:%li\n", metric[r].num_compar);
                printf("Num Memcpys:%li\n", metric[r].num_memcpys);
                printf("Num Runs:%li\n", metric[r].num_runs);

                /* Clean up and print final result*/
                free(buffer);
                if (0 != fclose(fp)) {
                    printf("Error file not closed!");
                }
                if (sorted)
                    printf("SUCCESS");
                else
                    printf("FAILURE");
                printf("\n\n");

            }
            
             /* Print Average Results*/
             int32_t value = 0;
             double v = 0;
             int32_t vals[7];
             printf("Time:\t\t");
             for (int i=0; i < numRuns; i++)
             {                
                printf("%li\t", (long) (metric[i].time));
                v+= metric[i].time;
             }
             printf("%li\n", (long) (v/numRuns));
             vals[0] = (long) (v/numRuns);

            v = 0;
            printf("GenTime:\t");
             for (int i=0; i < numRuns; i++)
             {                
                printf("%li\t", (long) (metric[i].genTime));
                v+= metric[i].genTime;
             }
             printf("%li\n", (long) (v/numRuns));
             vals[1] = (long) (v/numRuns);


            v = 0;
            printf("Runs:\t\t");
             for (int i=0; i < numRuns; i++)
             {                
                printf("%li\t", (long) (metric[i].num_runs));
                v+= metric[i].num_runs;
             }
             printf("%li\n", (long) (v/numRuns));
             vals[2] = (long) (v/numRuns);

             printf("Reads:\t\t");
             for (int i=0; i < numRuns; i++)
             {                
                printf("%li\t", metric[i].num_reads);
                value += metric[i].num_reads;
             }
             printf("%li\n", value/numRuns);
             vals[3] = value/numRuns;
             value = 0;
             printf("Writes: \t");
             for (int i=0; i < numRuns; i++)
             {                
                printf("%li\t", metric[i].num_writes);
                value += metric[i].num_writes;
             }
             printf("%li\n", value/numRuns);
             vals[4] = value/numRuns;
             value = 0;
             printf("Compares: \t");
             for (int i=0; i < numRuns; i++)
             {                
                printf("%li\t", metric[i].num_compar);
                value += metric[i].num_compar;
             }
             printf("%li\n", value/numRuns);
             vals[5] = value/numRuns;
             value = 0;
             printf("Copies: \t");
             for (int i=0; i < numRuns; i++)
             {                
                printf("%li\t", metric[i].num_memcpys);
                value += metric[i].num_memcpys;
             }
             printf("%li\n", value/numRuns);   
             vals[6] = value/numRuns;        
             printf("%li\t%li\t%li\t%li\t%li\t%li\t%li\n",vals[0], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6]);
        }
    }
}
