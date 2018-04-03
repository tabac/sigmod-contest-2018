#include <iostream>
#include <vector>
#include <unordered_map>
#include "Relation.hpp"
#include "Histogram.hpp"

#include "DataEngine.hpp"
#include "Plan.hpp"
#include "Mixins.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
//DataEngine::~DataEngine() {
//
//    for(unordered_map<HistKey, Histogram*>::iterator itr = histograms.begin(); itr != histograms.end(); itr++) {
//        delete itr->second;
//    }
//}
//---------------------------------------------------------------------------
void DataEngine::addRelation(RelationId relId, const char* fileName)
// Loads a relation from disk
{
    relations.emplace_back(relId, fileName);
}
//---------------------------------------------------------------------------
void DataEngine::buildCompleteHist(RelationId rid, int sampleRatio, int numOfBuckets) {
    #ifndef NDEBUG
    clock_t startTime = clock();
    #endif

    Relation& r = relations[rid];
    for(unsigned colID = 0; colID < r.columns.size(); colID++){
        // approx constructor
         Histogram* h = new Histogram(r, colID, r.size / sampleRatio);
         h->createEquiWidth(numOfBuckets);
         //h->createEquiHeight(numOfBuckets);

        //exact constructor
//        Histogram* h = new Histogram(r, colID);
//        h->createExactEquiWidth(numOfBuckets);

        histograms[pair<RelationId, unsigned>(rid, colID)] = h;
    }

    #ifndef NDEBUG
    cout << "Time taken: " << ((double)(clock() - startTime)/CLOCKS_PER_SEC) << endl;
    #endif
}
//---------------------------------------------------------------------------
Relation& DataEngine::getRelation(unsigned relationId)
// Loads a relation from disk
{
   if (relationId >= relations.size()) {
      cerr << "Relation with id: " << relationId << " does not exist" << endl;
      throw;
   }
   return relations[relationId];
}
//---------------------------------------------------------------------------
uint64_t DataEngine::getFilterEstimatedTuples(const FilterInfo& filter) {
    Histogram &h = *histograms.at(HistKey(filter.filterColumn.relId, filter.filterColumn.colId));
    if (filter.comparison == FilterInfo::Comparison::Less) {
        return h.getEstimatedKeys(0, filter.constant);
    } else if (filter.comparison == FilterInfo::Comparison::Greater) {
        return h.getEstimatedKeys(filter.constant, UINT64_MAX);
    }else{
        return h.getEstimatedKeys(filter.constant, filter.constant);
    }
}

void DataEngine::createSortedIndexes(void)
{
    vector<Relation>::iterator it;
    for (it = relations.begin(); it != relations.end(); ++it) {
        it->createIndex(it->columnsInfo[0]);
        if (it->columnsInfo.size() > 1) {
            it->createIndex(it->columnsInfo[1]);
        }
    }
}
//--------------------------------------------------------------------------
uint64_t DataEngine::getJoinEstimatedTuples(const PredicateInfo& predicate) {
    Histogram &hLeft = *histograms.at(HistKey(predicate.left.relId, predicate.left.colId));
    Histogram &hRight = *histograms.at(HistKey(predicate.right.relId, predicate.right.colId));
    uint64_t joinSize = 0;

    map<uint64_t, uint64_t>::iterator it;
    uint64_t prevBound = hLeft.domainMinimum;
    for (it = hLeft.histo.begin(); it != hLeft.histo.end(); it++) {
        joinSize += it->second * hRight.getEstimatedKeys(prevBound, it->first);
        prevBound = it->first;
    }
    return joinSize;
}
//--------------------------------------------------------------------------
float DataEngine::getFilterSelectivity(const FilterInfo& filter){
    return getFilterEstimatedTuples(filter) / (float) relations[filter.filterColumn.relId].size;
}
//--------------------------------------------------------------------------
float DataEngine::getJoinSelectivity(const PredicateInfo& predicate) {
    return getJoinEstimatedTuples(predicate)/(float)(relations[predicate.left.relId].size * relations[predicate.right.relId].size);
}
//--------------------------------------------------------------------------
