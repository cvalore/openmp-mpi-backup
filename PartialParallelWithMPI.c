#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdbool.h>
#include <omp.h>
#include <string.h>
#include <sys/time.h>
#include "mpi.h"

#define INITDIM 10000
#define LINELENGTH 700
#define PARCOUNT 30
#define MPI_DATATYPE_TUPLE 9
#define MPI_DATATYPE_MIN_MAX_DATE 1
#define SEPARATOR ','
#define QUOTES '"'
#define UNDEF "_UNDEF_"

typedef struct _tuple {
    int day;
    int month;
    int year;
    char time[8];
    char borough[16];
    int n_people_injured;
    int n_people_killed;
    int n_pedestrian_injured;
    int n_pedestrian_killed;
    int n_cyclist_injured;
    int n_cyclist_killed;
    int n_motor_injured;
    int n_motor_killed;
    char factor1[55];
    char factor2[55];
    char factor3[55];
    char factor4[55];
    char factor5[55];	
}Tuple;

typedef struct _minMaxDate {
    int minDay;
    int minMonth;
    int minYear;
    int maxDay;
    int maxMonth;
    int maxYear;
} MinMaxDate;

typedef struct _facList {
    char* facName;
    int facAccCount;
    int facLethCount;
    int prevCount;
    float percentage;
    struct _facList* next;
}FacList;

typedef struct _dateList {
    char* borName;
    int day;
    int month;
    int year;	
    int lethCount;
    struct _dateList* next;
}DateList;

typedef struct _borList {
    char* borName;
    //ORDERED LIST OF DATE
    DateList* dates;
    int* accidents;
    int lethals;
    struct _borList* next;
}BorList;

typedef struct _borName {
    char boroughName[16];
} BoroughName;

typedef struct _factName {
    char factorName[55];
} FactorName;

typedef struct _readInfo {
    int start_pos;
    int end_pos;
} ReadInfo;


double cpuSecond();
MPI_Datatype * defineMPITupleType(MPI_Datatype *);
MPI_Datatype * defineMPIMinMaxDateType(MPI_Datatype *);
MPI_Datatype * defineMBoroughNameType(MPI_Datatype *);
MPI_Datatype * defineFactorNameType(MPI_Datatype *);
//void readTheWholeTable(FILE * fin);
void processRow(char*);
void copyInto(Tuple*, Tuple*);
void processBorough(BorList* borPunt);
void initField(int, char*, Tuple*);
void parseDate(char*, int*, int*, int*);
int leapYear(int);
int nDayInMonth(int);
void compareMaxDate(int, int, int, int*, int*, int*);
void compareMinDate(int, int, int, int*, int*, int*);
char * selectFactor(Tuple *, int);
int killed(Tuple, int);
int nonLethAccidents(Tuple);
FacList* factFind(char*, FacList*);
BorList* borFind(char*, BorList*);
bool isBoroughPresent(char *, BoroughName *, int);
bool isFactorPresent(char *, FactorName *, int);
BorList * initializeBorList(BoroughName *, int);
//FacList * initializeFactList(FactorName *, int);
int * initializeEmptyAccidentArray();
int daysBetween(int, int, int, int, int, int);
int toEpochDay(int, int, int);
int weekInYears(int, int, int);

MinMaxDate minMaxDate = {-1, 1, -1, -1, -1, -1};
ReadInfo * localReadInfo;
int weeksNo = 0;
int count = 0;
int root = 0;
int maxdim = INITDIM;
Tuple* localTuples = NULL;
int world_rank;
int world_size;
int msgtag = 0;
MPI_Datatype Tupletype;

int main(int argc, char** argv) {
    //qui potremmo mettere un controllo sul rank del processo che sta eseguendo
    if (argc < 3) {
        printf("ERROR: Insert the name of the source file and the number of thread to run with\n");
	return -1;
    }

    omp_set_num_threads(atoi(argv[2]));
    //Data structures
    FILE *fin = fopen(argv[1], "r");

    //Setup the MPI environment
    MPI_Request req;
    MPI_Status stat;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    //Defining new MPI_Datatype corresponding to Tuple
    MPI_Datatype Tupletype;
    MPI_Type_commit(defineMPITupleType(&Tupletype));

    //Defining new MPI_Datatype corresponding to MinMaxDate
    MPI_Datatype MinMaxDateType;
    MPI_Type_commit(defineMPIMinMaxDateType(&MinMaxDateType));

    //Defining new MPI_Datatype corresponding to BoroughName
    MPI_Datatype BoroughNameType;
    MPI_Type_commit(defineMBoroughNameType(&BoroughNameType));

    //Defining new MPI_Datatype corresponding to FactorName
    MPI_Datatype FactorNameType;
    MPI_Type_commit(defineFactorNameType(&FactorNameType));

    int size_in_byte;
    int local_dim;


    double beginSetup = cpuSecond();

    localReadInfo = malloc(sizeof(ReadInfo));

    if (world_rank == 0) {
        fseek(fin, 0, SEEK_END);
        size_in_byte = ftell(fin);
        local_dim = size_in_byte / world_size;
        if (size_in_byte % world_size != 0){
            local_dim++;
        }
        ReadInfo * globalReadInfo = malloc(sizeof(ReadInfo) * world_size);
        for(int i = 0; i < world_size; i++) {
            globalReadInfo[i].start_pos = i * local_dim;
            globalReadInfo[i].end_pos = globalReadInfo[i].start_pos + local_dim;
        }
        //globalReadInfo[world_size-1].end_pos = size_in_byte;      maybe useless
        for (int i = 1; i < world_size; i++) {
            MPI_Isend(&(globalReadInfo[i]), 2, MPI_INT, i, 0, MPI_COMM_WORLD, &req);
        }
        localReadInfo->start_pos = globalReadInfo[root].start_pos;
        localReadInfo->end_pos = globalReadInfo[root].end_pos;

    } else {
        MPI_Recv(localReadInfo, 2, MPI_INT, root, 0, MPI_COMM_WORLD, &stat);
    }
    fclose(fin);

    //set the right position for each process
    //fseek(fin, localReadInfo->start_pos, SEEK_SET);
    localTuples = malloc(sizeof(Tuple) * INITDIM);

    double setupDuration = cpuSecond() - beginSetup;
    double beginLoading = cpuSecond();

#pragma omp parallel
    {
        int num_threads = omp_get_num_threads();
        int local_dimension = localReadInfo->end_pos - localReadInfo->start_pos;
        int thread_dim = local_dimension / num_threads;
        if (local_dimension % num_threads != 0){
            thread_dim++;
        }
        FILE* tmp = fopen(argv[1], "r");
        ReadInfo thread_read_info;
        thread_read_info.start_pos = localReadInfo->start_pos + omp_get_thread_num() * thread_dim;
        thread_read_info.end_pos = thread_read_info.start_pos + thread_dim;

        fseek(tmp, thread_read_info.start_pos, SEEK_SET);

        char buf[LINELENGTH];

        //Read and discard the first incomplete row
        fgets(buf, LINELENGTH, tmp);

//    if (buf == NULL) {
//        fscanf(fin, "\n");
//    }

        while(ftell(tmp) <= thread_read_info.end_pos) {         //CONTROLLARE L'UGUALE
            if(fgets(buf, LINELENGTH, tmp) != NULL) {
                processRow(buf);
            }
            else {
                break;
            }
        }
        fclose(tmp);
    }


    //Starting time of loading data section
    //readTheWholeTable(fin);
    //QUERY STUFFS

    //Find local oldest and newest date
    for (int i = 0; i < count; i++) {
        compareMaxDate(localTuples[i].day, localTuples[i].month, localTuples[i].year, &minMaxDate.maxDay, &minMaxDate.maxMonth, &minMaxDate.maxYear);
        compareMinDate(localTuples[i].day, localTuples[i].month, localTuples[i].year, &minMaxDate.minDay, &minMaxDate.minMonth, &minMaxDate.minYear);
    }


    MinMaxDate * minMaxDateList = NULL;

    minMaxDateList = malloc(sizeof(MinMaxDate) * world_size);

    //Collect local min and mix date and compute the global ones
    if (world_size > 1) {
        MPI_Gather(&minMaxDate, 1, MinMaxDateType, minMaxDateList, 1, MinMaxDateType, root, MPI_COMM_WORLD);
        if (world_rank == root) {
            for (int i = 0; i < world_size; i++) {
                compareMaxDate(minMaxDateList[i].maxDay, minMaxDateList[i].maxMonth, minMaxDateList[i].maxYear,
                               &minMaxDate.maxDay, &minMaxDate.maxMonth, &minMaxDate.maxYear);
                compareMinDate(minMaxDateList[i].minDay, minMaxDateList[i].minMonth, minMaxDateList[i].minYear,
                               &minMaxDate.minDay, &minMaxDate.minMonth, &minMaxDate.minYear);
            }
        }
        MPI_Bcast(&minMaxDate, 1, MinMaxDateType, 0, MPI_COMM_WORLD);
    }

    double durationLoading = cpuSecond() - beginLoading;

    //QUERY 1
    double beginQuery1 = cpuSecond();

    int daysNo = daysBetween(minMaxDate.minDay, minMaxDate.minMonth, minMaxDate.minYear, minMaxDate.maxDay, minMaxDate.maxMonth, minMaxDate.maxYear);
    weeksNo = daysNo / 7;
    if (daysNo != 0 && daysNo % 7 != 0)
        weeksNo++;

    int localLethalAccidentsData[weeksNo];
    int globalLethalAccidentsData[weeksNo];

    //Initialize a lock for each week to avoid race condition
    //omp_lock_t localLethalAccidentsDataLock[weeksNo];
    int totalLethalAccidents = 0;

    //Init
#pragma omp parallel for
    for (int i = 0; i < weeksNo; i++) {
        //omp_init_lock(&localLethalAccidentsDataLock[i]);
        localLethalAccidentsData[i] = 0;
    }


    //Define locally the num of accidents and of lethal accidents for each week
//#pragma omp parallel for
    for (int i = 0; i < count; i++) {
        if (localTuples[i].day != -1 && localTuples[i].month != -1 && localTuples[i].year != -1) {
            int index = weekInYears(localTuples[i].day, localTuples[i].month, localTuples[i].year);
            //omp_set_lock(&localLethalAccidentsDataLock[index]);
            localLethalAccidentsData[index] += killed(localTuples[i], 0);
            //omp_unset_lock(&localLethalAccidentsDataLock[index]);
        }
    }
/*
    //Destroy the used locks
    for (int i = 0; i < weeksNo; i++) {
        omp_destroy_lock(&localLethalAccidentsDataLock[i]);
    }
*/
    //Collect the local data in the root process
    if (world_size > 1) {
        MPI_Reduce(localLethalAccidentsData, globalLethalAccidentsData, weeksNo, MPI_INT, MPI_SUM, root,
                   MPI_COMM_WORLD);
    }

    double query1Creation = cpuSecond() - beginQuery1;
    double query1Begin_2 = cpuSecond();

    if (world_size > 1) {
        if (world_rank == root) {
            printf("\nQUERY 1:\n");
//#pragma omp parallel for
            for (int i = 0; i < weeksNo; i++) {
                printf("\tWeek%-5d%-10d%-2s\n", i + 1, globalLethalAccidentsData[i], "lethal accidents");
//#pragma omp critical
                {
                    totalLethalAccidents += globalLethalAccidentsData[i];
                }
            }
            printf("\t%d total lethal accidents over %d weeks, avg = %.2f%%\n", totalLethalAccidents, weeksNo,
                   (100.0f * totalLethalAccidents) / weeksNo);
        }
    } else{
        if (world_rank == root) {
            printf("\nQUERY 1:\n");
//#pragma omp parallel for
            for (int i = 0; i < weeksNo; i++) {
                printf("\tWeek%-5d%-10d%-2s\n", i + 1, localLethalAccidentsData[i], "lethal accidents");
//#pragma omp critical
                {
                    totalLethalAccidents += localLethalAccidentsData[i];
                }
            }
            printf("\t%d total lethal accidents over %d weeks, avg = %.2f%%\n", totalLethalAccidents, weeksNo,
                   (100.0f * totalLethalAccidents) / weeksNo);
        }
    }

    double query1Duration = cpuSecond() - query1Begin_2;
    //END OF QUERY 1


    //QUERY 2
    double beginQuery2 = cpuSecond();

    //Creation of the support list
    FacList *localFactList = malloc(sizeof(FacList));
    localFactList->next = NULL;
    localFactList->facName = NULL;
    localFactList->facAccCount = 0;
    localFactList->facLethCount = 0;
    localFactList->prevCount = -1;
    int factNum = 0;

    //Populate the support list
    for (int i = 0; i < count; i++) {
        FacList *ptrList[5] = {NULL};
        for (int j = 0; j < 5; j++) {
            char *currFactor = selectFactor(&localTuples[i], j);
            ptrList[j] = factFind(currFactor, localFactList);
            if (strcmp(currFactor, UNDEF) != 0) {
                if (ptrList[j] == NULL) {
                    //add to the localFactList a new elem
                    if (localFactList->facName == NULL) {
                        localFactList->facName = strdup(currFactor);
                        localFactList->facLethCount = killed(localTuples[i], 1);
                        localFactList->facAccCount = 1;
                        factNum++;
                    } else {
                        FacList *newEl = malloc(sizeof(FacList));
                        newEl->facName = strdup(currFactor);
                        newEl->facLethCount = killed(localTuples[i], 1);
                        newEl->facAccCount = 1;
                        newEl->next = localFactList;
                        localFactList = newEl;
                        factNum++;
                    }
                } else if (ptrList[j]->prevCount == ptrList[j]->facAccCount) {
                    ptrList[j]->facLethCount = ptrList[j]->facLethCount + killed(localTuples[i], 1);
                    ptrList[j]->facAccCount = ptrList[j]->facAccCount + 1;
                }
            } else {
                j = 5;
            }
        }

        localFactList->prevCount = localFactList->facAccCount;
        for (int k = 0; k < 5; k++) {
            if (ptrList[k] != NULL)
                ptrList[k]->prevCount = ptrList[k]->facAccCount;
        }
    }

    FacList *globalFactList;
    FactorName *receivedFactName;
    if (world_size > 1) {
        int factAllocatedSize = factNum * 2;                        //size to adjust?
        FactorName *factNames = malloc(sizeof(FactorName) * factAllocatedSize);

        //Define the local list of factor names
        FacList *ptr = localFactList;
        for (int j = 0; ptr != NULL; ++j) {
            strcpy(factNames[j].factorName, ptr->facName);
            ptr = ptr->next;
        }

        //Send to the root process the local list of factor names
        //The root process will update its own list and then forward the complete one to the other processes
        msgtag = 0;
        int realNumOfReceivedFactors;
        receivedFactName = malloc(sizeof(FactorName) * factAllocatedSize);
        if (world_rank != root) {
            MPI_Isend(factNames, factNum, FactorNameType, root, msgtag, MPI_COMM_WORLD, &req);
        } else {
            for (int i = 0; i < (world_size - 1); i++) {
                MPI_Recv(receivedFactName, factAllocatedSize, FactorNameType, MPI_ANY_SOURCE, msgtag, MPI_COMM_WORLD,
                         &stat);
                MPI_Get_count(&stat, FactorNameType, &realNumOfReceivedFactors);
                for (int j = 0; j < realNumOfReceivedFactors; ++j) {
                    if (!isFactorPresent(receivedFactName[j].factorName, factNames, factNum)) {
                        factNum++;
                        if (factNum == factAllocatedSize) {
                            factAllocatedSize = factNum + 10;
                            factNames = realloc(factNames, sizeof(FactorName) * factAllocatedSize);
                        }
                    }
                }
            }
        }


        MPI_Bcast(&factNum, 1, MPI_INT, root, MPI_COMM_WORLD);

        if (world_rank != root) {
            factNames = realloc(factNames, sizeof(FactorName) * factNum);
        }

        MPI_Bcast(factNames, factNum, FactorNameType, root, MPI_COMM_WORLD);


        //Create and initialize the global list of factors
        globalFactList = malloc(sizeof(FacList) *
                                         factNum);           //it does not work calling the ad hoc function initializeFactList..
#pragma parallel for
        for (int i = 0; i < factNum; ++i) {
            globalFactList[i].facName = strdup(factNames[i].factorName);
        }

        //Fill the global list of factors with data of all processes
        for (int i = 0; i < factNum; i++) {
            FacList *ptr = factFind(factNames[i].factorName, localFactList);
            if (ptr != NULL) {
                MPI_Reduce(&ptr->facAccCount, &globalFactList[i].facAccCount, 1, MPI_INT, MPI_SUM, root,
                           MPI_COMM_WORLD);
                MPI_Reduce(&ptr->facLethCount, &globalFactList[i].facLethCount, 1, MPI_INT, MPI_SUM, root,
                           MPI_COMM_WORLD);

            } else {
                int temp = 0;
                MPI_Reduce(&temp, &globalFactList[i].facAccCount, 1, MPI_INT, MPI_SUM, root, MPI_COMM_WORLD);
                MPI_Reduce(&temp, &globalFactList[i].facLethCount, 1, MPI_INT, MPI_SUM, root, MPI_COMM_WORLD);
            }
        }
    }
    double query2Creation = cpuSecond() - beginQuery2;
    double query2Begin_2 = cpuSecond();

    if (world_size > 1) {
        if (world_rank == root) {
            printf("\nQUERY 2:\n");
            printf("\t%-60s%-20s%-20s%s\n", "FACTOR", "N_ACCIDENTS", "N_DEATH", "PERC_N_DEATH");
            for (int i = 0; i < factNum; ++i) {
                if (globalFactList[i].facLethCount == 0 || globalFactList[i].facAccCount == 0)
                    globalFactList[i].percentage = 0;
                else
                    globalFactList[i].percentage =
                            (globalFactList[i].facLethCount * 100.0f) / globalFactList[i].facAccCount;
                printf("\t%-60s%-20d%-20d%.2f%%\n", globalFactList[i].facName, globalFactList[i].facAccCount,
                       globalFactList[i].facLethCount, globalFactList[i].percentage);
            }
        }
    } else {
        printf("\nQUERY 2:\n");
        printf("\t%-60s%-20s%-20s%s\n", "FACTOR", "N_ACCIDENTS", "N_DEATH", "PERC_N_DEATH");
        FacList * ptr = localFactList;
        while (ptr != NULL) {
            if (ptr->facLethCount == 0 || ptr->facAccCount == 0)
                ptr->percentage = 0;
            else
                ptr->percentage =
                        (ptr->facLethCount * 100.0f) / ptr->facAccCount;
            printf("\t%-60s%-20d%-20d%.2f%%\n", ptr->facName, ptr->facAccCount,
                   ptr->facLethCount, ptr->percentage);
            ptr = ptr->next;
        }
    }

    double query2Duration = cpuSecond() - query2Begin_2;
    //END OF QUERY 2


    //QUERY 3
    double beginQuery3 = cpuSecond();

    //Creation of the local support list
    BorList *localBorList = malloc(sizeof(BorList));
    localBorList->borName = NULL;
    localBorList->dates = NULL;
    localBorList->next = NULL;
    int boroughNum = 0;

    //Populate the local support list
    for (int i = 0; i < count; i++) {
        BorList *ptr = borFind(localTuples[i].borough, localBorList);
        if (strcmp(localTuples[i].borough, UNDEF) != 0) {
            if (ptr == NULL) {
                if (localBorList->borName == NULL) {
                    localBorList->borName = strdup(localTuples[i].borough);
                    localBorList->dates = malloc(sizeof(DateList));
                    localBorList->dates->day = localTuples[i].day;
                    localBorList->dates->month = localTuples[i].month;
                    localBorList->dates->year = localTuples[i].year;
                    localBorList->dates->lethCount = killed(localTuples[i], 0);
                    localBorList->dates->next = NULL;
                    boroughNum++;
                } else {
                    BorList *newEl = malloc(sizeof(BorList));
                    newEl->borName = strdup(localTuples[i].borough);
                    newEl->dates = malloc(sizeof(DateList));
                    newEl->dates->day = localTuples[i].day;
                    newEl->dates->month = localTuples[i].month;
                    newEl->dates->year = localTuples[i].year;
                    newEl->dates->lethCount = killed(localTuples[i], 0);
                    newEl->dates->next = NULL;
                    newEl->next = localBorList;
                    localBorList = newEl;
                    boroughNum++;
                }
            } else {
                DateList *newEl = malloc(sizeof(DateList));
                newEl->day = localTuples[i].day;
                newEl->month = localTuples[i].month;
                newEl->year = localTuples[i].year;
                newEl->lethCount = killed(localTuples[i], 0);
                newEl->next = ptr->dates;
                ptr->dates = newEl;
            }
        }
    }

    BorList *borPtr = localBorList;
    int allocatedSize = boroughNum * 10;
        BoroughName * boroughNames = malloc(sizeof(BoroughName) * allocatedSize);

        //Process the local data for each borough and populate the list of borough names
#pragma omp parallel
        {
#pragma omp single
            {
                for( int i = 0; borPtr != NULL && borPtr->borName != NULL; i++) {
#pragma omp task firstprivate(borPtr)
                    {
                        processBorough(borPtr);
                    }
                    strcpy(boroughNames[i].boroughName, borPtr->borName);
                    borPtr = borPtr->next;
                }
            }
        }

    BorList *globalBorList;
    BoroughName * receivedBoroughName;
    if (world_size > 1) {
        //Send to the root process the local list of borough names
        //The root process will update its own list and then forward the complete one to the other processes
        receivedBoroughName = malloc(
                sizeof(BoroughName) * allocatedSize);                 //size to adjust?
        msgtag = 0;
        int realNumOfReceivedBorough;
        if (world_rank != root) {
            MPI_Isend(boroughNames, boroughNum, BoroughNameType, root, msgtag, MPI_COMM_WORLD, &req);
        } else {
            for (int i = 0; i < (world_size - 1); i++) {
                MPI_Recv(receivedBoroughName, allocatedSize, BoroughNameType, MPI_ANY_SOURCE, msgtag, MPI_COMM_WORLD,
                         &stat);
                MPI_Get_count(&stat, BoroughNameType, &realNumOfReceivedBorough);
                for (int j = 0; j < realNumOfReceivedBorough; ++j) {
                    if (!isBoroughPresent(receivedBoroughName[j].boroughName, boroughNames, boroughNum)) {
                        boroughNum++;
                        if (boroughNum == allocatedSize) {
                            allocatedSize = boroughNum + 10;
                            boroughNames = realloc(boroughNames, sizeof(BoroughName) * allocatedSize);
                        }
                    }
                }
            }
        }

        MPI_Bcast(&boroughNum, 1, MPI_INT, root,
                  MPI_COMM_WORLD);                   //further checks on array dimensions are needed?

        if (world_rank != root) {
            boroughNames = realloc(boroughNames, sizeof(BoroughName) * boroughNum);
        }

        MPI_Bcast(boroughNames, boroughNum, BoroughNameType, root, MPI_COMM_WORLD);

        //Define and initialize the global list of boroughs with names of boroughs
        globalBorList = initializeBorList(boroughNames, boroughNum);

        //Fill the global list of boroughs with data of all processes
        for (int i = 0; i < boroughNum; i++) {
            BorList *ptr = borFind(boroughNames[i].boroughName, localBorList);
            if (ptr != NULL) {
                MPI_Reduce(ptr->accidents, globalBorList[i].accidents, weeksNo, MPI_INT, MPI_SUM, root, MPI_COMM_WORLD);
                MPI_Reduce(&(ptr->lethals), &globalBorList[i].lethals, 1, MPI_INT, MPI_SUM, root, MPI_COMM_WORLD);
            } else {
                int *acc = initializeEmptyAccidentArray();
                int temp = 0;
                MPI_Reduce(acc, globalBorList[i].accidents, weeksNo, MPI_INT, MPI_SUM, root, MPI_COMM_WORLD);
                MPI_Reduce(&temp, &globalBorList[i].lethals, 1, MPI_INT, MPI_SUM, root, MPI_COMM_WORLD);
                free(acc);
            }
        }
    }

    double query3Creation = cpuSecond() - beginQuery3;
    double query3Begin_2 = cpuSecond();

    if (world_size > 1) {
        if (world_rank == root) {
            printf("\nQUERY 3:\n");
            for (int i = 0; i < boroughNum; ++i) {
                printf("\tBOROUGH: %s\n", globalBorList[i].borName);
//#pragma omp parallel for
                for (int j = 0; j < weeksNo; j++) {
                    printf("\t\tWeek%-5d%-10d%-2s\n", 1 + j, globalBorList[i].accidents[j], "accidents");
                }
                printf("\t\tAvg lethal accidents/week: %.2f%% (%d lethal accidents over %d weeks)\n\n",
                       (100.0f * globalBorList[i].lethals) / weeksNo, globalBorList[i].lethals, weeksNo);
            }
        }
    } else {
        printf("\nQUERY 3:\n");
        BorList * ptr = localBorList;
        while (ptr != NULL) {
            printf("\tBOROUGH: %s\n", ptr->borName);
//#pragma omp parallel for
            for (int j = 0; j < weeksNo; j++) {
                printf("\t\tWeek%-5d%-10d%-2s\n", 1 + j, ptr->accidents[j], "accidents");
            }
            printf("\t\tAvg lethal accidents/week: %.2f%% (%d lethal accidents over %d weeks)\n\n",
                   (100.0f * ptr->lethals) / weeksNo, ptr->lethals, weeksNo);
            ptr = ptr->next;
        }
    }

    double query3Duration = cpuSecond() - query3Begin_2;
    //END OF QUERY 3

    //FREE DATA STRUCTURE
    double beginFree = cpuSecond();

    //Free local list of borough
    while (localBorList != NULL) {
        BorList *ptr = localBorList;
        localBorList = localBorList->next;
        free(ptr->borName);
        while (ptr->dates != NULL) {
            DateList *datePtr = ptr->dates;
            ptr->dates = ptr->dates->next;
            free(datePtr);
        }
        free(ptr->accidents);
        free(ptr);
    }

    //Free global list of borough
    if (world_size > 1) {
        for (int i = 0; globalBorList != NULL && i < boroughNum; ++i) {
            free(globalBorList[i].borName);
            free(globalBorList[i].accidents);
        }
        free(globalBorList);
    }
    free(boroughNames);

    //Free localFactList of factors
    while (localFactList != NULL) {
        FacList *ptr = localFactList;
        localFactList = localFactList->next;
        free(ptr->facName);
        free(ptr);
    }

    if (world_size > 1) {
        free(receivedBoroughName);
        free(receivedFactName);
    }
    free(minMaxDateList);
    free(localTuples);

    double freeDuration = cpuSecond() - beginFree;

    if (world_rank == root) {
        /*printf("\nIt took %f seconds to setup MPI communication\n", setupDuration);
        printf("\nIt took %f seconds to load data\n", durationLoading);
        printf("\nIt took %f seconds to calculate query 1\n", query1Creation);
        printf("\nIt took %f seconds to print query 1\n", query1Duration);
        printf("\nIt took %f seconds to calculate query 2\n", query2Creation);
        printf("\nIt took %f seconds to print query 2\n", query2Duration);
        printf("\nIt took %f seconds to calculate query 3\n", query3Creation);
        printf("\nIt took %f seconds to print query 3\n", query3Duration);
        printf("\nIt took %f seconds to free data structures\n", freeDuration);*/
    

   	//performances print
    	FILE* perfFile = fopen("parallel_performances_detail.txt", "a");
    	fprintf(perfFile, "%f\n", setupDuration);
    	fprintf(perfFile, "%f\n", durationLoading);
    	fprintf(perfFile, "%f\n", query1Creation);
    	fprintf(perfFile, "%f\n", query1Duration);
    	fprintf(perfFile, "%f\n", query2Creation);
    	fprintf(perfFile, "%f\n", query2Duration);
    	fprintf(perfFile, "%f\n", query3Creation);
    	fprintf(perfFile, "%f\n", query3Duration);
    	fprintf(perfFile, "%f\n", freeDuration);
    	fclose(perfFile);
	
    }

    MPI_Finalize();
    return 0;

}



MPI_Datatype * defineMPITupleType(MPI_Datatype * newType){

    int dim = MPI_DATATYPE_TUPLE;
    MPI_Datatype oldtypes[dim];
    MPI_Aint disp[dim];
    int blockcounts[dim];
    MPI_Aint int_extent, char_extent;

    MPI_Type_extent(MPI_INT, &int_extent);
    MPI_Type_extent(MPI_CHAR, &char_extent);

    // setup description of the 3 MPI_INT fields day, month, year
    disp[0] = 0;
    oldtypes[0] = MPI_INT;
    blockcounts[0] = 3;

    // setup description of the time attribute
    disp[1] = 3 * int_extent;
    oldtypes[1] = MPI_CHAR;
    blockcounts[1] = 8;

    // setup description of the borough attribute
    disp[2] = disp[1] + 8 * char_extent;
    oldtypes[2] = MPI_CHAR;
    blockcounts[2] = 16;

    // setup description of the 8 MPI_INT representing people killed or injured
    disp[3] = disp[2] + 16 * char_extent;
    oldtypes[3] = MPI_INT;
    blockcounts[3] = 8;

    // setup description of the contributing factor 1
    disp[4] = disp[3] + 8 * int_extent;
    oldtypes[4] = MPI_CHAR;
    blockcounts[4] = 55;

    // setup description of the contributing factor 2
    disp[5] = disp[4] + 55 * char_extent;
    oldtypes[5] = MPI_CHAR;
    blockcounts[5] = 55;

    // setup description of the contributing factor 3
    disp[6] = disp[5] + 55 * char_extent;
    oldtypes[6] = MPI_CHAR;
    blockcounts[6] = 55;

    // setup description of the contributing factor 4
    disp[7] = disp[6] + 55 * char_extent;
    oldtypes[7] = MPI_CHAR;
    blockcounts[7] = 55;

    // setup description of the contributing factor 5
    disp[8] = disp[7] + 55 * char_extent;
    oldtypes[8] = MPI_CHAR;
    blockcounts[8] = 55;

    MPI_Type_create_struct(dim, blockcounts, disp, oldtypes, newType);

    return newType;
}



MPI_Datatype * defineMPIMinMaxDateType(MPI_Datatype * newType) {

    int dim = MPI_DATATYPE_MIN_MAX_DATE;
    MPI_Datatype oldtypes[dim];
    MPI_Aint disp[dim];
    int blockcounts[dim];

    // setup description of the 3 MPI_INT fields day, month, year
    disp[0] = 0;
    oldtypes[0] = MPI_INT;
    blockcounts[0] = 6;

    MPI_Type_create_struct(dim, blockcounts, disp, oldtypes, newType);

    return newType;
}


MPI_Datatype * defineMBoroughNameType(MPI_Datatype * newType) {
    int dim = 1;
    MPI_Datatype oldtypes[dim];
    MPI_Aint disp[dim];
    int blockcounts[dim];

    // setup description of the boroughName attribute
    disp[0] = 0;
    oldtypes[0] = MPI_CHAR;
    blockcounts[0] = 16;

    MPI_Type_create_struct(dim, blockcounts, disp, oldtypes, newType);

    return newType;
}


MPI_Datatype * defineFactorNameType(MPI_Datatype * newType) {
    int dim = 1;
    MPI_Datatype oldtypes[dim];
    MPI_Aint disp[dim];
    int blockcounts[dim];

    // setup description of the factorName attribute
    disp[0] = 0;
    oldtypes[0] = MPI_CHAR;
    blockcounts[0] = 55;

    MPI_Type_create_struct(dim, blockcounts, disp, oldtypes, newType);

    return newType;
}

/*
void readTheWholeTable(FILE * fin){
    char buf[LINELENGTH];
    bool beyond_size = false;

    //Read and discard the first incomplete row
    fgets(buf, LINELENGTH, fin);
    //fscanf(fin, "%m[^\n]\n", &buf);
//    if (buf == NULL) {
//        fscanf(fin, "\n");
//    }
//Read the whole table line by line
#pragma omp parallel
    {
#pragma omp single
        {
            while (fgets(buf, LINELENGTH, fin) != NULL && !beyond_size) {
#pragma omp task firstprivate(buf)
                {
                    processRow(buf);
                }
                if (ftell(fin) > end_pos) {
                    beyond_size = true;
                }
            }
        }
    }
    fclose(fin);
}
*/

void processRow(char* row){

    //Parse the line char by char
    //int len = strlen(row) + 2;
    int len = strlen(row);
    //row = realloc(row, len);
    row[len - 1] = ',';
    row[len] = '\0';
    char *sup = malloc(len + 1);
    strcpy(sup, "");
    int index = 0;
    int paramCount = 0;
    int insideQuotes = 0;
    Tuple toInsert;


    for (int i = 0; i < len; i++) {
        if (row[i] == QUOTES) {
            if (insideQuotes == 0)
                insideQuotes = 1;
            else
                insideQuotes = 0;
        }
        if (row[i] == SEPARATOR && insideQuotes == 0) {
            sup[index] = '\0';
            index = 0;
            initField(paramCount, sup, &toInsert);
            paramCount++;
            strcpy(sup, "");
        } else {
            sup[index] = row[i];
            index++;
        }
    }

    free(sup);

    //free(row);
    Tuple* toAdd;
#pragma omp critical
    {
        toAdd = &(localTuples[count]);
        count++;
        copyInto(toAdd, &toInsert);             //POTREBBE ESSERE UNSAFE?
        if (count == maxdim) {
            maxdim = maxdim * 2;
            localTuples = realloc(localTuples, sizeof(Tuple) * maxdim);
        }
    }

}


void copyInto(Tuple* tuple, Tuple* tupleToInsert){
    tuple->day = tupleToInsert->day;
    tuple->month = tupleToInsert->month;
    tuple->year = tupleToInsert->year;
    strcpy(tuple->time, tupleToInsert->time);
    strcpy(tuple->borough, tupleToInsert->borough);
    tuple->n_people_injured = tupleToInsert->n_people_injured;
    tuple->n_people_killed = tupleToInsert->n_people_killed;
    tuple->n_pedestrian_injured = tupleToInsert->n_pedestrian_injured;
    tuple->n_pedestrian_killed = tupleToInsert->n_pedestrian_killed;
    tuple->n_cyclist_injured = tupleToInsert->n_cyclist_injured;
    tuple->n_cyclist_killed = tupleToInsert->n_cyclist_killed;
    tuple->n_motor_injured = tupleToInsert->n_motor_injured;
    tuple->n_motor_killed = tupleToInsert->n_motor_killed;
    strcpy(tuple->factor1, tupleToInsert->factor1);
    strcpy(tuple->factor2, tupleToInsert->factor2);
    strcpy(tuple->factor3, tupleToInsert->factor3);
    strcpy(tuple->factor4, tupleToInsert->factor4);
    strcpy(tuple->factor5, tupleToInsert->factor5);
}


void processBorough(BorList* borPunt){

    borPunt->accidents = malloc(sizeof(int) * weeksNo);
    borPunt->lethals = 0;
    //init array
#pragma parallel for
    for (int i = 0; i < weeksNo; i++)
        borPunt->accidents[i] = 0;

    //Populate array
    DateList* datePtr = borPunt->dates;
    while (datePtr != NULL) {
        if (datePtr->day != -1 && datePtr->month != -1 && datePtr->year != -1) {
            int index = weekInYears(datePtr->day, datePtr->month, datePtr->year);
            borPunt->accidents[index] = borPunt->accidents[index] + 1;
            borPunt->lethals += datePtr->lethCount;
        }

        datePtr = datePtr->next;
    }
}

bool isBoroughPresent(char * boroughName, BoroughName * boroughNames, int boroughNamesSize){
    for (int i = 0; i < boroughNamesSize; ++i) {
        if (strcmp(boroughName, boroughNames[i].boroughName) == 0)
            return true;
    }
    strcpy(boroughNames[boroughNamesSize].boroughName, boroughName);
    return false;
}


bool isFactorPresent(char * factorName, FactorName * factorNames, int factorNameSize) {
    for (int i = 0; i < factorNameSize; ++i) {
        if (strcmp(factorName, factorNames[i].factorName) == 0)
            return true;
    }
    strcpy(factorNames[factorNameSize].factorName, factorName);
    return false;
}


BorList * initializeBorList(BoroughName * boroughNames, int size) {
    BorList * borList = malloc(sizeof(BorList) * size);
#pragma parallel for
    for (int i = 0; i < size; ++i) {
        borList[i].borName = strdup(boroughNames[i].boroughName);
        borList[i].accidents = malloc(sizeof(int) * weeksNo);
    }
    return borList;
}


/*
FacList * initializeFactList(FactorName * factNames, int size){
    FacList * facList = malloc(sizeof(facList) * size);
#pragma parallel for
    for (int i = 0; i < size; ++i) {
        facList[i].facName = strdup(factNames[i].factorName);
    }
    return facList;
}
*/


int * initializeEmptyAccidentArray() {
    int * accidents = malloc(sizeof(int) * weeksNo);
#pragma parallel for
    for (int i = 0; i < weeksNo; ++i) {
        accidents[i] = 0;
    }
    return accidents;
}


int weekInYears(int day, int month, int year) {
    int gap = daysBetween(minMaxDate.minDay, minMaxDate.minMonth, minMaxDate.minYear, day, month, year);
    //useless but just to be sure
    if(gap == 0)
        return 0;
    if(gap % 7 == 0)
        return ((gap/7) - 1);
    return gap/7;
}

//starting and ending date INCLUDED
int daysBetween(int sDay, int sMonth, int sYear, int eDay, int eMonth, int eYear) {
    return 1 + toEpochDay(eDay, eMonth, eYear) - toEpochDay(sDay, sMonth, sYear);
}

//taken from the implementation of JAVA
int toEpochDay(int day, int month, int year) {
    int y = year;
    int m = month;
    int total = 0;
    total += 365 * y;
    if (y >= 0) {
        total += (y + 3) / 4 - (y + 99) / 100 + (y + 399) / 400;
    }
    else {
        total -= y / -4 - y / -100 + y / -400;
    }

    total += (367 * m - 362) / 12;
    total += day - 1;
    if (m > 2) {
        --total;
        if (leapYear(year) == 0) {
            --total;
        }
    }

    return total - 719528;
}

char * selectFactor(Tuple * currTuple, int position){
    switch (position) {
        case 0:
            return currTuple->factor1;
        case 1:
            return currTuple->factor2;
        case 2:
            return currTuple->factor3;
        case 3:
            return currTuple->factor4;
        case 4:
            return currTuple->factor5;
    }
}

FacList* factFind(char* str, FacList* list) {
    FacList* ptr = list;
    if(ptr->facName == NULL) {
        return NULL;
    }
    while(ptr != NULL) {
        if(strcmp(str, ptr->facName) == 0)
            return ptr;
        ptr = ptr->next;
    }
    return NULL;
}

BorList* borFind(char* str, BorList* list) {
    BorList* ptr = list;
    if(ptr->borName == NULL) {
        return NULL;
    }
    while(ptr != NULL) {
        if(strcmp(str, ptr->borName) == 0)
            return ptr;
        ptr = ptr->next;
    }
    return NULL;
}

void compareMaxDate(int day, int month, int year, int* maxDay, int* maxMonth, int* maxYear) {
    if(day == -1 || month == -1 || year == -1)
        return;
    if(*maxDay == -1 || *maxMonth == -1 || *maxYear == -1) {
        *maxDay = day;
        *maxMonth = month;
        *maxYear = year;
        return;
    }
    if(year > *maxYear) {
        *maxDay = day;
        *maxMonth = month;
        *maxYear = year;
        return;
    }
    if(year == *maxYear) {
        if(month > *maxMonth) {
            *maxDay = day;
            *maxMonth = month;
            return;
        }
        if(month == *maxMonth) {
            if(day > *maxDay) {
                *maxDay = day;
            }
        }
    }
}

void compareMinDate(int day, int month, int year, int* minDay, int* minMonth, int* minYear) {
    if(day == -1 || month == -1 || year == -1)
        return;
    if(*minDay == -1 || *minMonth == -1 || *minYear == -1) {
        *minDay = day;
        *minMonth = month;
        *minYear = year;
        return;
    }
    if(year < *minYear) {
        *minDay = day;
        *minMonth = month;
        *minYear = year;
        return;
    }
    if(year == *minYear) {
        if(month < *minMonth) {
            *minDay = day;
            *minMonth = month;
            return;
        }
        if(month == *minMonth) {
            if(day < *minDay) {
                *minDay = day;
            }
        }
    }
}

int nonLethAccidents(Tuple t) {
    int n5 = t.n_people_injured;
    int n6 = t.n_pedestrian_injured;
    int n7 = t.n_motor_injured;
    int n8 = t.n_cyclist_injured;
    if(n5 < 0)	n5 = 0;
    if(n6 < 0) 	n6 = 0;
    if(n7 < 0)	n7 = 0;
    if(n8 < 0)	n8 = 0;
    return n5+n6+n7+n8;
}


int killed(Tuple t, int flag) {
    int n1 = t.n_people_killed;
    int n2 = t.n_pedestrian_killed;
    int n3 = t.n_motor_killed;
    int n4 = t.n_cyclist_killed;
    if(n1 < 0)	n1 = 0;
    if(n2 < 0) 	n2 = 0;
    if(n3 < 0)	n3 = 0;
    if(n4 < 0)	n4 = 0;

    if(!flag)
        return (n1||n2||n3||n4);
    return n1+n2+n3+n4;
}

int leapYear(int year){
    //the year is not a leap year
    if(year % 4 != 0) {
        return 0;
    }
    if(year % 100 != 0) {
        return 1;
    }
    if(year % 400 == 0) {
        return 1;
    }
    return 0;
}

int nDayInMonth(int month) {
    switch(month) {
        case 1:
            return 31;
        case 2:
            return 28;
        case 3:
            return 31;
        case 4:
            return 30;
        case 5:
            return 31;
        case 6:
            return 30;
        case 7:
            return 31;
        case 8:
            return 31;
        case 9:
            return 30;
        case 10:
            return 31;
        case 11:
            return 30;
        case 12:
            return 31;
        defualt:
        {
            printf("ERROR: Cannot calculate day in this month (%d) properly\n", month);
            return -1;
        }
    }
}

void parseDate(char* str, int* d, int* m, int* y) {
    char* day;
    char* month;
    char* year;
    sscanf(str, "%m[^/]/%m[^/]/%ms", &month, &day, &year);
    *d = atoi(day);
    *m = atoi(month);
    *y = atoi(year);
    free(day);
    free(month);
    free(year);
}

double cpuSecond(){
    struct timeval tp;
    gettimeofday(&tp, NULL);
    return ((double)tp.tv_sec+(double)tp.tv_usec*1.e-6);
}

void initField(int paramCount, char* param, Tuple* t) {
    switch (paramCount) {

        case 0: {
            if(strcmp(param, "") != 0) {
                parseDate(param, &(t->day), &(t->month), &(t->year));
            }
            else {
                t->day = -1;
                t->month = -1;
                t->year = -1;
            }
        }break;
        case 1: {
            if(strcmp(param, "") != 0)
                strcpy(t->time, param);
            else
                strcpy(t->time, UNDEF);
        }break;
        case 2: {
            if(strcmp(param, "") != 0)
                strcpy(t->borough, param);
            else
                strcpy(t->borough, UNDEF);
        }break;
        case 3: break;
        case 4: break;
        case 5: break;
        case 6: break;
        case 7: break;
        case 8: break;
        case 9: break;
        case 10: {
            if(strcmp(param, "") != 0)
                t->n_people_injured = atoi(param);
            else
                t->n_people_injured = -1;
        }break;
        case 11: {
            if(strcmp(param, "") != 0)
                t->n_people_killed = atoi(param);
            else
                t->n_people_killed = -1;
        }break;
        case 12: {
            if(strcmp(param, "") != 0)
                t->n_pedestrian_injured = atoi(param);
            else
                t->n_pedestrian_injured = -1;
        }break;
        case 13: {
            if(strcmp(param, "") != 0)
                t->n_pedestrian_killed = atoi(param);
            else
                t->n_pedestrian_killed = -1;
        }break;
        case 14: {
            if(strcmp(param, "") != 0)
                t->n_cyclist_injured = atoi(param);
            else
                t->n_cyclist_injured = -1;
        }break;
        case 15: {
            if(strcmp(param, "") != 0)
                t->n_cyclist_killed = atoi(param);
            else
                t->n_cyclist_killed = -1;
        }break;
        case 16: {
            if(strcmp(param, "") != 0)
                t->n_motor_injured = atoi(param);
            else
                t->n_motor_injured = -1;
        }break;
        case 17: {
            if(strcmp(param, "") != 0)
                t->n_motor_killed = atoi(param);
            else
                t->n_motor_killed = -1;
        }break;
        case 18: {
            if(strcmp(param, "") != 0)
                strcpy(t->factor1, param);
            else
                strcpy(t->factor1, UNDEF);
        }break;
        case 19: {
            if(strcmp(param, "") != 0)
                strcpy(t->factor2, param);
            else
                strcpy(t->factor2, UNDEF);
        }break;
        case 20: {
            if(strcmp(param, "") != 0)
                strcpy(t->factor3, param);
            else
                strcpy(t->factor3, UNDEF);
        }break;
        case 21: {
            if(strcmp(param, "") != 0)
                strcpy(t->factor4, param);
            else
                strcpy(t->factor4, UNDEF);
        }break;
        case 22: {
            if(strcmp(param, "") != 0)
                strcpy(t->factor5, param);
            else
                strcpy(t->factor5, UNDEF);
        }break;
        case 23: break;
        case 24: break;
        case 25: break;
        case 26: break;
        case 27: break;
        case 28: break;
    }
}

