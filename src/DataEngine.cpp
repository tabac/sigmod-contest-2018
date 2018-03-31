#include <vector>
#include <iostream>
#include "DataEngine.hpp"
#include "Plan.hpp"
#include "Mixins.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void DataEngine::addRelation(RelationId relId, const char* fileName)
// Loads a relation from disk
{
    this->relations.emplace_back(relId, fileName);
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
void DataEngine::createSortedIndexes(void)
{
    vector<Relation>::iterator it;
    for (it = this->relations.begin(); it != this->relations.end(); ++it) {
        it->createIndex(it->columnsInfo[0]);
        if (it->columnsInfo.size() > 1) {
            it->createIndex(it->columnsInfo[1]);
        }
    }
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
float DataEngine::getFilterSelectivity(FilterOperatorNode*, DataNode &){
    return 0.5;
}
//--------------------------------------------------------------------------
float DataEngine::getJoinSelectivity(JoinOperatorNode*, DataNode &){
   return 0;
}
//--------------------------------------------------------------------------
