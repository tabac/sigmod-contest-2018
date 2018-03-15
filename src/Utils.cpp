#include <iostream>
#include <vector>
#include "Utils.hpp"
#include "Parser.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
static void createColumn(vector<uint64_t*>& columns,uint64_t numTuples)
  // Create a dummy column
{
  auto col=new uint64_t[numTuples];
  columns.push_back(col);
  for (unsigned i=0;i<numTuples;++i) {
    col[i]=i;
  }
}
//---------------------------------------------------------------------------
Relation Utils::createRelation(RelationId relId, uint64_t size,uint64_t numColumns)
  // Create a dummy relation
{
  vector<uint64_t*> columns;
  for (unsigned i=0;i<numColumns;++i) {
    createColumn(columns,size);
  }
  return Relation(relId, size,move(columns));
}
//---------------------------------------------------------------------------
void Utils::storeRelation(ofstream& out,Relation& r,unsigned i)
  // Store a relation in all formats
{
  auto baseName = "r" + to_string(i);
  r.storeRelation(baseName);
  r.storeRelationCSV(baseName);
  r.dumpSQL(baseName, i);
  out << baseName << "\n";

  DEBUGLN(baseName);
}
//---------------------------------------------------------------------------
