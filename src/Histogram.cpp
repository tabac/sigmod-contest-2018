#include <vector>
#include <cstdlib>
#include <iostream>
#include <cmath>
#include <Histogram.hpp>
//---------------------------------------------------------------------------

using namespace std;

//---------------------------------------------------------------------------
Histogram::Histogram(Relation& r, unsigned colId):r(r), colId(colId){
    sample = NULL;
}
//---------------------------------------------------------------------------
Histogram::Histogram(Relation& r, unsigned colId, uint64_t budget):r(r), colId(colId), budget(budget){
    srand((unsigned)time(0));
    sample = new uint64_t[budget];
    for(uint64_t i = 0; i < budget; i++){
        sample[i] = r.columns[colId][rand() % r.size];
    }
}
//---------------------------------------------------------------------------
Histogram::~Histogram() {
    if(sample!=NULL){
        delete[] sample;
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

    #ifndef NDEBUG
        cout <<"Hist on r"<< this->r.relId <<",c"<< this->colId <<endl;
        printHistogram();
    #endif

    delete[] sample;
    sample = NULL;
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

    domainMinimum = minVal;
    uint64_t width = (maxVal-minVal)/numberOfBuckets;

    uint64_t* bucketIndexing = new uint64_t[numberOfBuckets+1];
    //uint64_t* bucketIndexing = new uint64_t[(int)ceil((maxVal-minVal)/width)+1];
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

    #ifndef NDEBUG
        cout <<"Hist on r"<< this->r.relId <<",c"<< this->colId <<endl;
        cout << "Width: " << width << endl;
        cout << "MinVal: " << minVal << ", MaxVal: " << maxVal << endl;
        printHistogram();
    #endif
}

//---------------------------------------------------------------------------
void Histogram::createExactEquiWidth(int numberOfBuckets) {

    uint64_t minVal=UINT64_MAX, maxVal=0;
    vector<uint64_t>::iterator begin (r.columns[colId]);
    vector<uint64_t>::iterator end (r.columns[colId] + r.size);

    vector<uint64_t>::iterator colIter;
    for(colIter = begin; colIter!=end; colIter++){
        if(*colIter > maxVal){
            maxVal = *colIter;
        }
        if(*colIter < minVal){
            minVal = *colIter;
        }
    }

    domainMinimum = minVal;
    uint64_t width = (maxVal-minVal)/numberOfBuckets;

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
        histo[bucketKey]++;
    }

    delete[] bucketIndexing;
    //delete[] sample;

    #ifndef NDEBUG
        cout <<"Hist on r"<< this->r.relId <<",c"<< this->colId <<endl;
        cout << "Exact Histo" << endl;
        cout << "MinVal: " << minVal << ", MaxVal: " << maxVal << endl;
        cout << "Width: " << width << endl;
        printHistogram();
    #endif
}
//---------------------------------------------------------------------------

uint64_t Histogram::getEstimatedKeys(uint64_t lb, uint64_t ub) {
    #ifndef NDEBUG
        //cout << "Histo Query: (" << lb << "," << ub << ")\n";
    #endif

    if(ub == UINT64_MAX){
        ub = (--histo.end())->first;
    }

    float selectedKeys = 0;
    map<uint64_t,uint64_t>::iterator it;
    for(it = histo.begin(); it != histo.end(); it++){
        if(it->first >= lb){
            break;
        }
    }
    uint64_t prevKey = (--it)->first;
    for(it = histo.lower_bound(lb); it != histo.lower_bound(ub); it++){
        #ifndef NDEBUG
           // cout << "Checking in range: [" << prevKey << "," << it->first << ")\n";
        #endif

        selectedKeys+= (it->first-lb)/(float)(it->first-prevKey) * it->second;
        lb = it->first;
        prevKey = it->first;
    }
    it = histo.lower_bound(ub);
    if(it != histo.end()) {
        #ifndef NDEBUG
           // cout << "Checking in range: [" << prevKey << "," << it->first << ")\n";
        #endif

        selectedKeys += (ub - prevKey) / (float)(it->first - prevKey) * it->second;
    }

    return (uint64_t) selectedKeys;
}

//---------------------------------------------------------------------------
#ifndef NDEBUG
void Histogram::printHistogram() {
    for( const auto& hpair : histo )
    {
        cout << hpair.first << "-->" << hpair.second << "\n" ;
    }
}
#endif

