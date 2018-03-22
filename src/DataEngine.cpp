#include <vector>
#include <iostream>
#include <time.h>
#include "DataEngine.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
DataEngine::~DataEngine() {
    unordered_map<HistKey , Histogram>::iterator it;
    for(it = histograms.begin(); it != histograms.end(); it++){
        delete &(it->second);
    }
}
//---------------------------------------------------------------------------
void DataEngine::addRelation(RelationId relId, const char* fileName)
// Loads a relation from disk
{
   this->relations.emplace_back(relId, fileName);
}
//---------------------------------------------------------------------------
void DataEngine::buildCompleteHist(RelationId rid, int sampleRatio, int numOfBuckets) {
    clock_t startTime = clock();

    Relation& r = this->relations[rid];
    for(unsigned colID = 0; colID < r.columns.size(); colID++){
        Histogram& h = *new Histogram(r, colID, r.size / sampleRatio);
        //h.createEquiWidth(numOfBuckets);
        //h.createExactEquiWidth(numOfBuckets);
        h.createEquiHeight(numOfBuckets);
        this->histograms.insert(pair<HistKey, Histogram> (pair<RelationId, unsigned>(rid, colID), h));
    }

    cout << "Time taken: " << ((double)(clock() - startTime)/CLOCKS_PER_SEC) << endl;
}
//---------------------------------------------------------------------------
Relation& DataEngine::getRelation(unsigned relationId)
// Loads a relation from disk
{
   if (relationId >= this->relations.size()) {
      cerr << "Relation with id: " << relationId << " does not exist" << endl;
      throw;
   }
   return this->relations[relationId];
}
//---------------------------------------------------------------------------
float DataEngine::getEstimatedSelectivity(AbstractOperatorNode &op, DataNode &d){
    FilterOperatorNode* filterOp;
    JoinOperatorNode* joinOp;
    if ((filterOp = dynamic_cast<FilterOperatorNode*>(&op)) != NULL){
        cout << "Estimate for Filter operator" << endl;
        return getFilterSelectivity(filterOp, d);
    }else if ((joinOp = dynamic_cast<JoinOperatorNode*>(&op)) != NULL){
        cout << "Estimate for Join operator" << endl;
        return getJoinSelectivity(joinOp, d);
    }else{
       cout << "Uknown operator";
       return 0;
    }
}
//---------------------------------------------------------------------------
float DataEngine::getFilterSelectivity(FilterOperatorNode* filterOp, DataNode &d){
    SelectInfo &si = (filterOp -> info).filterColumn;
    RelationId r = si.relId;
    //TODO:check statistics for this select and filter Info
    //for now I just return a zero-selectivity.
    return 0.5;
}
//--------------------------------------------------------------------------
float DataEngine::getJoinSelectivity(JoinOperatorNode* joinOp, DataNode &d){
   return 0;
}

