#include <vector>
#include <cstdlib>
#include <iostream>
#include <cmath>
#include <Histogram.hpp>
//---------------------------------------------------------------------------

using namespace std;

//---------------------------------------------------------------------------
Histogram::Histogram(Relation& r, unsigned colId):r(r), colId(colId){}
//---------------------------------------------------------------------------
Histogram::Histogram(Relation& r, unsigned colId, uint64_t budget):r(r), colId(colId), budget(budget){
    srand((unsigned)time(0));
    sample = new uint64_t[budget];
    for(uint64_t i = 0; i < budget; i++){
        sample[i] = r.columns[colId][rand() % r.size];
    }
}
//---------------------------------------------------------------------------
void Histogram::createEquiHeight(int numberOfBuckets) {

    int invSampleRatio = (int) ceil(r.size / budget);
    float histogramHeight = ceil((float) budget / numberOfBuckets) * invSampleRatio;

    sort(sample, sample+budget);
    domainMinimum = sample[0];

    int freq = 0;
    uint64_t bucketUpperBound = 0;
    for(uint64_t i = 0; i < budget; i++){
        bucketUpperBound = sample[i];
        freq += invSampleRatio;
        if(freq == histogramHeight){
            histo[bucketUpperBound] = freq;
            freq = 0;
        }
    }
    if(freq != 0){
        histo[sample[budget-1]] = freq;
    }

    printHistogram();

    delete[] sample;
}
//---------------------------------------------------------------------------

void Histogram::createEquiWidth(int numberOfBuckets) {

    int invSampleRatio = (int) ceil(r.size / budget);
    uint64_t minVal=UINT64_MAX, maxVal=0;
    vector<uint64_t>::iterator begin (sample);
    vector<uint64_t>::iterator end (sample + budget);

    vector<uint64_t>::iterator colIter;
    for(colIter = begin; colIter!=end; colIter++){
        if(*colIter > maxVal){
            maxVal = *colIter;
        }
        if(*colIter < minVal){
            minVal = *colIter;
        }
    }

    cout << "MinVal: " << minVal << ", MaxVal: " << maxVal << endl;

    uint64_t width = (maxVal-minVal)/numberOfBuckets;

    cout << "Width: " << width << endl;

    uint64_t* bucketIndexing = new uint64_t[numberOfBuckets+1];
    int bIndex = 0;
    for(uint64_t i = minVal+width; i < maxVal; i+=width){
        histo[i] = 0;
        bucketIndexing[bIndex] = i;
        bIndex++;
    }
    histo[maxVal] = 0;
    bucketIndexing[bIndex] = maxVal;


    for(colIter = begin; colIter!=end; colIter++){
        uint64_t bucketKey = bucketIndexing[(*colIter-minVal)/width];
        histo[bucketKey]+=invSampleRatio;
    }

    delete[] bucketIndexing;
    delete[] sample;

    printHistogram();
}

//---------------------------------------------------------------------------

uint64_t Histogram::getEstimatedKeys(uint64_t lb, uint64_t ub) {
    cout << "Histo Query: (" << lb << "," << ub << ")\n";

    uint64_t selectedKeys = 0;
    map<uint64_t,uint64_t>::iterator it;
    uint64_t prevKey = domainMinimum;
    for(it = histo.upper_bound(lb); it != histo.upper_bound(ub); it++){
        cout << "Checking in range: [" << prevKey << "," << it->first << ")\n";

        selectedKeys+= (it->first-lb)/(it->first-prevKey) * it->second;
        lb = it->first;
        prevKey = it->first;
    }
    it = histo.upper_bound(ub);
    if(it != histo.end()) {
        cout << "Checking in range: [" << prevKey << "," << it->first << ")\n";

        selectedKeys += (ub - prevKey) / (it->first - prevKey) * it->second;
    }

    return selectedKeys;
}

//---------------------------------------------------------------------------

void Histogram::printHistogram() {
    for( const auto& hpair : histo )
    {
        cout << hpair.first << "-->" << hpair.second << "\n" ;
    }
}

