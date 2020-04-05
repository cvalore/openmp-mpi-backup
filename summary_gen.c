#include <stdio.h>
#include <stdlib.h>

int main(int argc, char** argv) {

	if(argc < 5) {
		printf("Insert first the name of the file where to take data from\n");
		printf("Then the name of the file to which save output\n");
		printf("Then the number of iterations\n");
		printf("Then the number of parameters\n");
		return -1;
	}

	FILE* fin = fopen(argv[1], "r");
	FILE* fout = fopen(argv[2], "w");
	int numIter = atoi(argv[3]);
	int numParam = atoi(argv[4]);

	double perf[numParam];
	for(int i = 0; i < numParam; i++) {
		perf[i] = 0.0f;
	}

	for(int i = 0; i < numIter; i++) {
		for(int j = 0; j < numParam; j++) {
			double read = 0.0f;
			fscanf(fin, "%lf\n", &read);
			perf[j] += read;
		}
	}

	fprintf(fout, "In average after %d iteration:\n", numIter);
	if(numParam == 8) {
	fprintf(fout, "Loading data time avg: %f\n", perf[0]/numIter);
	fprintf(fout, "Query 1 calculation time avg: %f\n", perf[1]/numIter);
	fprintf(fout, "Query 1 print time avg: %f\n", perf[2]/numIter);
	fprintf(fout, "Query 2 support creation time avg: %f\n", perf[3]/numIter);
	fprintf(fout, "Query 2 calculation and print time avg: %f\n", perf[4]/numIter);
	fprintf(fout, "Query 3 support creation time avg: %f\n", perf[5]/numIter);
	fprintf(fout, "Query 3 calculation and print time avg: %f\n", perf[6]/numIter);
	fprintf(fout, "Free structures time avg: %f\n", perf[7]/numIter);
	}
	else {
	fprintf(fout, "Setup MPI data time avg: %f\n", perf[0]/numIter);
	fprintf(fout, "Loading data time avg: %f\n", perf[1]/numIter);
	fprintf(fout, "Query 1 calculation time avg: %f\n", perf[2]/numIter);
	fprintf(fout, "Query 1 print time avg: %f\n", perf[3]/numIter);
	fprintf(fout, "Query 2 support creation time avg: %f\n", perf[4]/numIter);
	fprintf(fout, "Query 2 calculation and print time avg: %f\n", perf[5]/numIter);
	fprintf(fout, "Query 3 support creation time avg: %f\n", perf[6]/numIter);
	fprintf(fout, "Query 3 calculation and print time avg: %f\n", perf[7]/numIter);
	fprintf(fout, "Free structures time avg: %f\n", perf[8]/numIter);
	}

	fclose(fin);
	fclose(fout);
	return 0;
}
