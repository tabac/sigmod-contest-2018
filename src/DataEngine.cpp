#include <vector>
#include <iostream>
#include "DataEngine.hpp"
#include "Histogram.hpp"
#include "Plan.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void DataEngine::addRelation(RelationId relId, const char* fileName)
// Loads a relation from disk
{
   this->relations.emplace_back(relId, fileName);
}
//---------------------------------------------------------------------------
void DataEngine::buildCompleteHist(RelationId rid, int sampleRatio, int numOfBuckets) {
    Relation& r = this->relations[rid];
    for(unsigned colID = 0; colID < r.columns.size(); colID++){
        Histogram* h = new Histogram(r, colID, r.size / sampleRatio);
        h->createEquiWidth(numOfBuckets);
        Histogram& hist = *h;
        this->histograms[pair(rid, colID)] = hist;
    }
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

