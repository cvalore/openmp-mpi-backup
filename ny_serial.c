#include <stdio.h>
#include <stdlib.h>
#include <omp.h>
#include <string.h>
#include <sys/time.h>

#define INITDIM 10000
#define PARCOUNT 30
#define SEPARATOR ','
#define QUOTES '"'
#define UNDEF "_UNDEF_"

typedef struct _tuple {
	int day;
	int month;
	int year;
	char* time;
	char* borough;
	int zip;
	char* latitude;
	char* longitude;
	char* location;
	char* on_street_name;
	char* cross_street_name;
	char* off_street_name;
	int n_people_injured;
	int n_people_killed;
	int n_pedestrian_injured;
	int n_pedestrian_killed;
	int n_cyclist_injured;
	int n_cyclist_killed;
	int n_motor_injured;
	int n_motor_killed;
	char* factor1;
	char* factor2;
	char* factor3;
	char* factor4;
	char* factor5;
	int key;
	char* vehicle_type1;
	char* vehicle_type2;
	char* vehicle_type3;
	char* vehicle_type4;
	char* vehicle_type5;
}Tuple;

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
	//int accCount;
	int lethCount;
	struct _dateList* next;
}DateList;

typedef struct _borList {
	char* borName;
	//ORDERED LIST OF DATE
	DateList* dates;
	int* accidents;
	int lethals;
	//int minWeek;
	struct _borList* next;
}BorList;


double cpuSecond();
void processRow(char*);
void copyInto(Tuple*, Tuple*);
void processBorough(BorList* borPunt);
void initField(int, char*, Tuple*);
void parseDate(char*, int*, int*, int*);
int leapYear(int);
int nDayInMonth(int);
void compareMaxDate(int, int, int, int*, int*, int*);
void compareMinDate(int, int, int, int*, int*, int*);
int killed(Tuple, int);
int nonLethAccidents(Tuple);
FacList* find(char*, FacList*);
BorList* borFind(char*, BorList*);
int daysBetween(int, int, int, int, int, int);
int toEpochDay(int, int, int);
int weekInYears(int, int, int);

int minYear = -1;
int minMonth = -1;
int minDay = -1;
int maxYear = -1;
int maxMonth = -1;
int maxDay = -1;
int weeksNo = 0;
int count = 0;
int maxdim = INITDIM;
Tuple* tuples = NULL;

int main(int argc, char** argv) {
    if (argc < 2) {
        printf("ERROR: Insert the name of the source file\n");
        return -1;
    }

    //Data structures
    FILE *fin = fopen(argv[1], "r");
    char *buf;
    tuples = malloc(sizeof(Tuple) * INITDIM);
    //Read and discard the first row (identifiers names)
    fscanf(fin, "%m[^\n]\n", &buf);
    free(buf);

    //Starting time of loading data section
    double beginLoading = cpuSecond();

    //Read the whole table line by line
    while (fscanf(fin, "%m[^\n]\n", &buf) != EOF) {
        processRow(buf);
    }
     

    fclose(fin);

    //Find oldest and newest date
    for (int i = 0; i < count; i++) {
        compareMaxDate(tuples[i].day, tuples[i].month, tuples[i].year, &maxDay, &maxMonth, &maxYear);
        compareMinDate(tuples[i].day, tuples[i].month, tuples[i].year, &minDay, &minMonth, &minYear);
    }
	
	double durationLoading = cpuSecond() - beginLoading;

    //QUERY STUFFS

    double beginQuery1 = cpuSecond();
	
    int daysNo = daysBetween(minDay, minMonth, minYear, maxDay, maxMonth, maxYear);
    weeksNo = daysNo / 7;
    if (daysNo != 0 && daysNo % 7 != 0)
        weeksNo++;

    int lethalAccidentsData[weeksNo];
    int totalLethalAccidents = 0;

    //init
    for (int i = 0; i < weeksNo; i++) {
        lethalAccidentsData[i] = 0;
    }


    for (int i = 0; i < count; i++) {
        if (tuples[i].day != -1 && tuples[i].month != -1 && tuples[i].year != -1) {
            int index = weekInYears(tuples[i].day, tuples[i].month, tuples[i].year);
            lethalAccidentsData[index] += killed(tuples[i], 0);
        }
    }

  
    double query1Creation = cpuSecond() - beginQuery1;
    double query1Begin_2 = cpuSecond();

    printf("\nQUERY 1:\n");
    for (int i = 0; i < weeksNo; i++) {
        printf("\tWeek%-5d%-10d%-2s\n", i + 1, lethalAccidentsData[i], "lethal accidents");
    	totalLethalAccidents += lethalAccidentsData[i];
    }
    printf("\t%d total lethal accidents over %d weeks, avg = %.2f%%\n", totalLethalAccidents, weeksNo,
           (100.0f * totalLethalAccidents) / weeksNo);

    double query1Duration = cpuSecond() - query1Begin_2;
    //END OF QUERY 1


    //QUERY 2

    double beginQuery2 = cpuSecond();

    //Creation of the support list
    FacList *list = malloc(sizeof(FacList));
    list->next = NULL;
    list->facName = NULL;
    list->facAccCount = 0;
    list->facLethCount = 0;
    list->prevCount = -1;

    for (int i = 0; i < count; i++) {
        FacList *ptr1 = find(tuples[i].factor1, list);
        if (strcmp(tuples[i].factor1, UNDEF) != 0) {
            if (ptr1 == NULL) {
                //add to the list a new elem
                if (list->facName == NULL) {
                    list->facName = strdup(tuples[i].factor1);
                    list->facLethCount = killed(tuples[i], 1);
                    //list->facAccCount = list->facLethCount + nonLethAccidents(tuples[i]);
                    list->facAccCount = 1;
                } else {
                    FacList *newEl = malloc(sizeof(FacList));
                    newEl->facName = strdup(tuples[i].factor1);
                    newEl->facLethCount = killed(tuples[i], 1);
                    //newEl->facAccCount = newEl->facLethCount + nonLethAccidents(tuples[i]);
                    newEl->facAccCount = 1;
                    newEl->next = list;
                    list = newEl;
                }
            } else if (ptr1->prevCount == ptr1->facAccCount) {
                ptr1->facLethCount = ptr1->facLethCount + killed(tuples[i], 1);
                //ptr1->facAccCount = ptr1->facAccCount + killed(tuples[i]) + nonLethAccidents(tuples[i]);
                ptr1->facAccCount = ptr1->facAccCount + 1;
            }
        }

        FacList *ptr2 = find(tuples[i].factor2, list);
        if (strcmp(tuples[i].factor2, UNDEF) != 0) {
            if (ptr2 == NULL) {
                //add to the list a new elem
                if (list->facName == NULL) {
                    list->facName = strdup(tuples[i].factor2);
                    list->facLethCount = killed(tuples[i], 1);
                    //list->facAccCount = list->facLethCount + nonLethAccidents(tuples[i]);
                    list->facAccCount = 1;
                } else {
                    FacList *newEl = malloc(sizeof(FacList));
                    newEl->facName = strdup(tuples[i].factor2);
                    newEl->facLethCount = killed(tuples[i], 1);
                    //newEl->facAccCount = newEl->facLethCount + nonLethAccidents(tuples[i]);
                    newEl->facAccCount = 1;
                    newEl->next = list;
                    list = newEl;
                }
            } else if (ptr2->prevCount == ptr2->facAccCount) {
                ptr2->facLethCount = ptr2->facLethCount + killed(tuples[i], 1);
                //ptr2->facAccCount = ptr2->facAccCount + killed(tuples[i]) + nonLethAccidents(tuples[i]);
                ptr2->facAccCount = ptr2->facAccCount + 1;
            }
        }

        FacList *ptr3 = find(tuples[i].factor3, list);
        if (strcmp(tuples[i].factor3, UNDEF) != 0) {
            if (ptr3 == NULL) {
                //add to the list a new elem
                if (list->facName == NULL) {
                    list->facName = strdup(tuples[i].factor3);
                    list->facLethCount = killed(tuples[i], 1);
                    //list->facAccCount = list->facLethCount + nonLethAccidents(tuples[i]);
                    list->facAccCount = 1;
                } else {
                    FacList *newEl = malloc(sizeof(FacList));
                    newEl->facName = strdup(tuples[i].factor3);
                    newEl->facLethCount = killed(tuples[i], 1);
                    //newEl->facAccCount = newEl->facLethCount + nonLethAccidents(tuples[i]);
                    newEl->facAccCount = 1;
                    newEl->next = list;
                    list = newEl;
                }
            } else if (ptr3->prevCount == ptr3->facAccCount) {
                ptr3->facLethCount = ptr3->facLethCount + killed(tuples[i], 1);
                //ptr3->facAccCount = ptr3->facAccCount + killed(tuples[i]) + nonLethAccidents(tuples[i]);
                ptr3->facAccCount = ptr3->facAccCount + 1;
            }
        }

        FacList *ptr4 = find(tuples[i].factor4, list);
        if (strcmp(tuples[i].factor4, UNDEF) != 0) {
            if (ptr4 == NULL) {
                //add to the list a new elem
                if (list->facName == NULL) {
                    list->facName = strdup(tuples[i].factor4);
                    list->facLethCount = killed(tuples[i], 1);
                    //list->facAccCount = list->facLethCount + nonLethAccidents(tuples[i]);
                    list->facAccCount = 1;
                } else {
                    FacList *newEl = malloc(sizeof(FacList));
                    newEl->facName = strdup(tuples[i].factor4);
                    newEl->facLethCount = killed(tuples[i], 1);
                    //newEl->facAccCount = newEl->facLethCount + nonLethAccidents(tuples[i]);
                    newEl->facAccCount = 1;
                    newEl->next = list;
                    list = newEl;
                }
            } else if (ptr4->prevCount == ptr4->facAccCount) {
                ptr4->facLethCount = ptr4->facLethCount + killed(tuples[i], 1);
                //ptr4->facAccCount = ptr4->facAccCount + killed(tuples[i]) + nonLethAccidents(tuples[i]);
                ptr4->facAccCount = ptr4->facAccCount + 1;
            }
        }

        FacList *ptr5 = find(tuples[i].factor5, list);
        if (strcmp(tuples[i].factor5, UNDEF) != 0) {
            if (ptr5 == NULL) {
                //add to the list a new elem
                if (list->facName == NULL) {
                    list->facName = strdup(tuples[i].factor5);
                    list->facLethCount = killed(tuples[i], 1);
                    //list->facAccCount = list->facLethCount + nonLethAccidents(tuples[i]);
                    list->facAccCount = 1;
                } else {
                    FacList *newEl = malloc(sizeof(FacList));
                    newEl->facName = strdup(tuples[i].factor5);
                    newEl->facLethCount = killed(tuples[i], 1);
                    //newEl->facAccCount = newEl->facLethCount + nonLethAccidents(tuples[i]);
                    newEl->facAccCount = 1;
                    newEl->next = list;
                    list = newEl;
                }
            } else if (ptr5->prevCount == ptr5->facAccCount) {
                ptr5->facLethCount = ptr5->facLethCount + killed(tuples[i], 1);
                //ptr5->facAccCount = ptr5->facAccCount + killed(tuples[i]) + nonLethAccidents(tuples[i]);
                ptr5->facAccCount = ptr5->facAccCount + 1;
            }
        }

        list->prevCount = list->facAccCount;
        if (ptr1 != NULL)
            ptr1->prevCount = ptr1->facAccCount;
        if (ptr2 != NULL)
            ptr2->prevCount = ptr2->facAccCount;
        if (ptr3 != NULL)
            ptr3->prevCount = ptr3->facAccCount;
        if (ptr4 != NULL)
            ptr4->prevCount = ptr4->facAccCount;
        if (ptr5 != NULL)
            ptr5->prevCount = ptr5->facAccCount;
    }

    double query2Creation = cpuSecond() - beginQuery2;
    double query2Begin_2 = cpuSecond();

    FacList *ptr = list;
    printf("\nQUERY 2:\n");
    printf("\t%-60s%-20s%-20s%s\n", "FACTOR", "N_ACCIDENTS", "N_DEATH", "PERC_N_DEATH");
    while (ptr != NULL) {
        if (ptr->facLethCount == 0 || ptr->facAccCount == 0)
            ptr->percentage = 0;
        else
            ptr->percentage = (ptr->facLethCount * 100.0f) / ptr->facAccCount;
        //printf("\t%s |\t%d |\t%d |\t%.2f%%|\n", ptr->facName, ptr->facAccCount, ptr->facLethCount, ptr->percentage);
        printf("\t%-60s%-20d%-20d%.2f%%\n", ptr->facName, ptr->facAccCount, ptr->facLethCount, ptr->percentage);
        ptr = ptr->next;
    }

    double query2Duration = cpuSecond() - query2Begin_2;

    //END OF QUERY 2


    //QUERY 3

    double beginQuery3 = cpuSecond();

    BorList *borList = malloc(sizeof(BorList));
    borList->borName = NULL;
    borList->dates = NULL;
    borList->next = NULL;

    for (int i = 0; i < count; i++) {
        BorList *ptr = borFind(tuples[i].borough, borList);
        if (strcmp(tuples[i].borough, UNDEF) != 0) {
            if (ptr == NULL) {
                if (borList->borName == NULL) {
                    borList->borName = strdup(tuples[i].borough);

                    borList->dates = malloc(sizeof(DateList));
                    borList->dates->day = tuples[i].day;
                    borList->dates->month = tuples[i].month;
                    borList->dates->year = tuples[i].year;
                    borList->dates->lethCount = killed(tuples[i], 0);
                    //borList->dates->accCount = borList->dates->lethCount + nonLethAccidents(tuples[i]);
                    //borList->dates->accCount = 1;
                    borList->dates->next = NULL;
                } else {
                    BorList *newEl = malloc(sizeof(BorList));
                    newEl->borName = strdup(tuples[i].borough);

                    newEl->dates = malloc(sizeof(DateList));
                    newEl->dates->day = tuples[i].day;
                    newEl->dates->month = tuples[i].month;
                    newEl->dates->year = tuples[i].year;
                    newEl->dates->lethCount = killed(tuples[i], 0);
                    //newEl->dates->accCount = newEl->dates->lethCount + nonLethAccidents(tuples[i]);
                    //newEl->dates->accCount = 1;
                    newEl->dates->next = NULL;

                    newEl->next = borList;
                    borList = newEl;
                }
            } else {
                DateList *newEl = malloc(sizeof(DateList));
                newEl->day = tuples[i].day;
                newEl->month = tuples[i].month;
                newEl->year = tuples[i].year;
                newEl->lethCount = killed(tuples[i], 0);
                //newEl->accCount = newEl->lethCount + nonLethAccidents(tuples[i]);
                //newEl->accCount = 1;

                newEl->next = ptr->dates;
                ptr->dates = newEl;
            }
        }
    }

    BorList *borPtr = borList;
    
       
    while (borPtr != NULL && borPtr->borName != NULL) {    	
	processBorough(borPtr);
        borPtr = borPtr->next;
    }

    

	double query3Creation = cpuSecond() - beginQuery3;
	double query3Begin_2 = cpuSecond();

	printf("\nQUERY 3:\n");
	borPtr = borList;
	while(borPtr != NULL && borPtr->borName != NULL) {

		printf("\tBOROUGH: %s\n", borPtr->borName);

		for(int i = 0; i < weeksNo; i++) {
			printf("\t\tWeek%-5d%-10d%-2s\n", 1+i, borPtr->accidents[i], "accidents");
		}

		printf("\t\tAvg lethal accidents/week: %.2f%% (%d lethal accidents over %d weeks)\n\n", (100.0f*borPtr->lethals)/weeksNo, borPtr->lethals, weeksNo);

		borPtr = borPtr->next;
	}

	double query3Duration = cpuSecond() - query3Begin_2;

	//END OF QUERY 3

	//FREE DATA STRUCTURE

	double beginFree = cpuSecond();

	//Free list of borough
	while(borList != NULL) {
		BorList* ptr = borList;
		borList = borList->next;
		free(ptr->borName);
		while(ptr->dates != NULL) {
			DateList* datePtr = ptr->dates;
			ptr->dates = ptr->dates->next;
			free(datePtr);
		}
		free(ptr->accidents);
		free(ptr);
	}

	//Free list of factors
	while(list != NULL) {
		FacList* ptr = list;
		list = list->next;
		free(ptr->facName);
		free(ptr);
	}

	//Free tuples
	for(int i = 0; i < count; i++) {
		free(tuples[i].time);
		free(tuples[i].borough);
		free(tuples[i].latitude);
		free(tuples[i].longitude);
		free(tuples[i].location);
		free(tuples[i].on_street_name);
		free(tuples[i].cross_street_name);
		free(tuples[i].off_street_name);
		free(tuples[i].factor1);
		free(tuples[i].factor2);
		free(tuples[i].factor3);
		free(tuples[i].factor4);
		free(tuples[i].factor5);
		free(tuples[i].vehicle_type1);
		free(tuples[i].vehicle_type2);
		free(tuples[i].vehicle_type3);
		free(tuples[i].vehicle_type4);
		free(tuples[i].vehicle_type5);
	}
	free(tuples);
	double freeDuration = cpuSecond() - beginFree;


	printf("\nIt took %f seconds to load data\n", durationLoading);
	printf("\nIt took %f seconds to calculate query 1\n", query1Creation);
	printf("\nIt took %f seconds to print query 1\n", query1Duration);
	printf("\nIt took %f seconds to create support list for query 2\n", query2Creation);
	printf("\nIt took %f seconds to calculate and print query 2\n", query2Duration);
	printf("\nIt took %f seconds to create support list for query 3\n", query3Creation);
	printf("\nIt took %f seconds to calculate and print query 3\n", query3Duration);
	printf("\nIt took %f seconds to free data structures\n", freeDuration);

	
	//print to performances file
	
	/*FILE* perfFile = fopen("serial_performances_detail.txt", "a");

	fprintf(perfFile, "%f\n", durationLoading);
	fprintf(perfFile, "%f\n", query1Creation);
	fprintf(perfFile, "%f\n", query1Duration);
	fprintf(perfFile, "%f\n", query2Creation);
	fprintf(perfFile, "%f\n", query2Duration);
	fprintf(perfFile, "%f\n", query3Creation);
	fprintf(perfFile, "%f\n", query3Duration);
	fprintf(perfFile, "%f\n", freeDuration);

    	fclose(perfFile);*/

	return 0;
}


void processRow(char* row){

	//Parse the line char by char
	int len = strlen(row) + 2;
	row = realloc(row, len);
	row[len - 2] = ',';
	row[len - 1] = '\0';
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

	free(row);
	Tuple* toAdd;

	toAdd = &(tuples[count]);
	count++;
        copyInto(toAdd, &toInsert);
        if (count == maxdim) {
            maxdim = maxdim * 2;
            tuples = realloc(tuples, sizeof(Tuple) * maxdim);
        }
    
}


void copyInto(Tuple* tuple, Tuple* tupleToInsert){
	tuple->day = tupleToInsert->day;
	tuple->month = tupleToInsert->month;
	tuple->year = tupleToInsert->year;
	tuple->time = tupleToInsert->time;
	tuple->borough = tupleToInsert->borough;
	tuple->zip = tupleToInsert->zip;
	tuple->latitude = tupleToInsert->latitude;
	tuple->longitude = tupleToInsert->longitude;
	tuple->location = tupleToInsert->location;
	tuple->on_street_name = tupleToInsert->on_street_name;
	tuple->cross_street_name = tupleToInsert->cross_street_name;
	tuple->off_street_name = tupleToInsert->off_street_name;
	tuple->n_people_injured = tupleToInsert->n_people_injured;
	tuple->n_people_killed = tupleToInsert->n_people_killed;
	tuple->n_pedestrian_injured = tupleToInsert->n_pedestrian_injured;
	tuple->n_pedestrian_killed = tupleToInsert->n_pedestrian_killed;
	tuple->n_cyclist_injured = tupleToInsert->n_cyclist_injured;
	tuple->n_cyclist_killed = tupleToInsert->n_cyclist_killed;
	tuple->n_motor_injured = tupleToInsert->n_motor_injured;
	tuple->n_motor_killed = tupleToInsert->n_motor_killed;
	tuple->factor1 = tupleToInsert->factor1;
	tuple->factor2 = tupleToInsert->factor2;
	tuple->factor3 = tupleToInsert->factor3;
	tuple->factor4 = tupleToInsert->factor4;
	tuple->factor5 = tupleToInsert->factor5;
	tuple->key = tupleToInsert->key;
	tuple->vehicle_type1 = tupleToInsert->vehicle_type1;
	tuple->vehicle_type2 = tupleToInsert->vehicle_type2;
	tuple->vehicle_type3 = tupleToInsert->vehicle_type3;
	tuple->vehicle_type4 = tupleToInsert->vehicle_type4;
	tuple->vehicle_type5 = tupleToInsert->vehicle_type5;
}


void processBorough(BorList* borPunt){
   
    borPunt->accidents = malloc(sizeof(int) * weeksNo);
    borPunt->lethals = 0;
    //init array
    for (int i = 0; i < weeksNo; i++)
        borPunt->accidents[i] = 0;

    //Populate array
    DateList* datePtr = borPunt->dates;
    while (datePtr != NULL) {
        if (datePtr->day != -1 && datePtr->month != -1 && datePtr->year != -1) {
            int index = weekInYears(datePtr->day, datePtr->month, datePtr->year);
            //borPunt->accidents[index] += datePtr->accCount;
            borPunt->accidents[index] = borPunt->accidents[index] + 1;
            borPunt->lethals += datePtr->lethCount;
        }

        datePtr = datePtr->next;
    }
}

int weekInYears(int day, int month, int year) {
	int gap = daysBetween(minDay, minMonth, minYear, day, month, year);
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

FacList* find(char* str, FacList* list) {
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
				t->time = strdup(param);
			else
				t->time = strdup(UNDEF);
		}break;
		case 2: {
			if(strcmp(param, "") != 0)
				t->borough = strdup(param);
			else
				t->borough = strdup(UNDEF);
		}break;
		case 3: {
			if(strcmp(param, "") != 0)
				t->zip = atoi(param);
			else
				t->zip = -1;
		}break;
		case 4: {
			if(strcmp(param, "") != 0)
				t->latitude = strdup(param);
			else
				t->latitude = strdup(UNDEF);
		}break;
		case 5: {
			if(strcmp(param, "") != 0)
				t->longitude = strdup(param);
			else
				t->longitude = strdup(UNDEF);
		}break;
		case 6: {
			if(strcmp(param, "") != 0)
				t->location = strdup(param);
			else
				t->location = strdup(UNDEF);
		}break;
		case 7: {
			if(strcmp(param, "") != 0)
				t->on_street_name = strdup(param);
			else
				t->on_street_name = strdup(UNDEF);
		}break;
		case 8: {
			if(strcmp(param, "") != 0)
				t->cross_street_name = strdup(param);
			else
				t->cross_street_name = strdup(UNDEF);
		}break;
		case 9: {
			if(strcmp(param, "") != 0)
				t->off_street_name = strdup(param);
			else
				t->off_street_name = strdup(UNDEF);
		}break;
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
				t->factor1 = strdup(param);
			else
				t->factor1 = strdup(UNDEF);
		}break;
		case 19: {
			if(strcmp(param, "") != 0)
				t->factor2= strdup(param);
			else
				t->factor2 = strdup(UNDEF);
		}break;
		case 20: {
			if(strcmp(param, "") != 0)
				t->factor3 = strdup(param);
			else
				t->factor3 = strdup(UNDEF);
		}break;
		case 21: {
			if(strcmp(param, "") != 0)
				t->factor4 = strdup(param);
			else
				t->factor4 = strdup(UNDEF);
		}break;
		case 22: {
			if(strcmp(param, "") != 0)
				t->factor5 = strdup(param);
			else
				t->factor5 = strdup(UNDEF);
		}break;
		case 23: {
			if(strcmp(param, "") != 0)
				t->key = atoi(param);
			else
				t->key = -1;
		}break;
		case 24: {
			if(strcmp(param, "") != 0)
				t->vehicle_type1 = strdup(param);
			else
				t->vehicle_type1 = strdup(UNDEF);
		}break;
		case 25: {
			if(strcmp(param, "") != 0)
				t->vehicle_type2 = strdup(param);
			else
				t->vehicle_type2 = strdup(UNDEF);
		}break;
		case 26: {
			if(strcmp(param, "") != 0)
				t->vehicle_type3 = strdup(param);
			else
				t->vehicle_type3 = strdup(UNDEF);
		}break;
		case 27: {
			if(strcmp(param, "") != 0)
				t->vehicle_type4 = strdup(param);
			else
				t->vehicle_type4 = strdup(UNDEF);
		}break;
		case 28: {
			if(strcmp(param, "") != 0)
				t->vehicle_type5 = strdup(param);
			else
				t->vehicle_type5 = strdup(UNDEF);
		}break;
		default: {
			/*printf("BEGIN ERROR: Cannot initialize field properly\n");
            printf("Arguments: %d | %s\n", paramCount, param);
            printf("Tuple: ");
            printTuple(*t);
            printf("\nEND ERROR count %d\n", count);*/
			break;
		}
	}
}
