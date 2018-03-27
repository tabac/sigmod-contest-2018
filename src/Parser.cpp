#include <cassert>
#include <iostream>
#include <utility>
#include <sstream>
#include <unordered_set>
#include "Parser.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void FilterInfo::getFilteredIndices(const IteratorPair &valIter,
                                    vector<uint64Pair> &indices)
// Returns the indices of `valIter` that satisfy `this` condition.
{
    uint64_t c;
    vector<uint64_t>::const_iterator it;

    switch (this->comparison) {
        case FilterInfo::Comparison::Less:
            for (c = 0, it = valIter.first; it != valIter.second; ++it, ++c) {
                if ((*it) < this->constant) {
                    indices.emplace_back(c, 0);
                }
            }
            break;
        case FilterInfo::Comparison::Equal:
            for (c = 0, it = valIter.first; it != valIter.second; ++it, ++c) {
                if ((*it) == this->constant) {
                    indices.emplace_back(c, 0);
                }
            }
            break;
        case FilterInfo::Comparison::Greater:
            for (c = 0, it = valIter.first; it != valIter.second; ++it, ++c) {
                if ((*it) > this->constant) {
                    indices.emplace_back(c, 0);
                }
            }
            break;
    }
}
//---------------------------------------------------------------------------
static void splitString(string& line,vector<unsigned>& result,const char delimiter)
// Split a line into numbers
{
    stringstream ss(line);
    string token;
    while (getline(ss,token,delimiter)) {
        result.push_back(stoul(token));
    }
}
//---------------------------------------------------------------------------
static void splitString(string& line,vector<string>& result,const char delimiter)
// Parse a line into strings
{
    stringstream ss(line);
    string token;
    while (getline(ss,token,delimiter)) {
        result.push_back(token);
    }
}
//---------------------------------------------------------------------------
static void splitPredicates(string& line,vector<string>& result)
// Split a line into predicate strings
{
    // Determine predicate type
    for (auto cT : comparisonTypes) {
        if (line.find(cT)!=string::npos) {
            splitString(line,result,cT);
            break;
        }
    }
}
//---------------------------------------------------------------------------
void QueryInfo::parseRelationIds(string& rawRelations)
    // Parse a string of relation ids
{
    splitString(rawRelations,relationIds,' ');
}
//---------------------------------------------------------------------------
static SelectInfo parseRelColPair(string& raw)
{
    vector<unsigned> ids;
    splitString(raw,ids,'.');
    return SelectInfo(0,ids[0],ids[1]);
}
//---------------------------------------------------------------------------
inline static bool isConstant(string& raw) { return raw.find('.')==string::npos; }
//---------------------------------------------------------------------------
void QueryInfo::parsePredicate(string& rawPredicate)
    // Parse a single predicate: join "r1Id.col1Id=r2Id.col2Id" or "r1Id.col1Id=constant" filter
{
    vector<string> relCols;
    splitPredicates(rawPredicate,relCols);
    assert(relCols.size()==2);
    assert(!isConstant(relCols[0])&&"left side of a predicate is always a SelectInfo");
    auto leftSelect=parseRelColPair(relCols[0]);
    if (isConstant(relCols[1])) {
        uint64_t constant=stoul(relCols[1]);
        char compType=rawPredicate[relCols[0].size()];
        filters.emplace_back(leftSelect,constant,FilterInfo::Comparison(compType));
    } else {
        predicates.emplace_back(leftSelect,parseRelColPair(relCols[1]));
    }
}
//---------------------------------------------------------------------------
void QueryInfo::parsePredicates(string& text)
// Parse predicates
{
    vector<string> predicateStrings;
    splitString(text,predicateStrings,'&');
    for (auto& rawPredicate : predicateStrings) {
        parsePredicate(rawPredicate);
    }
}
//---------------------------------------------------------------------------
void QueryInfo::parseSelections(string& rawSelections)
 // Parse selections
{
    vector<string> selectionStrings;
    splitString(rawSelections,selectionStrings,' ');
    for (auto& rawSelect : selectionStrings) {
        selections.emplace_back(parseRelColPair(rawSelect));
    }
}
//---------------------------------------------------------------------------
static void resolveIds(vector<unsigned>& relationIds,SelectInfo& selectInfo)
    // Resolve relation id
{
    selectInfo.relId=relationIds[selectInfo.binding];
}
//---------------------------------------------------------------------------
void QueryInfo::resolveRelationIds()
    // Resolve relation ids
{
    // Selections
    for (auto& sInfo : selections) {
        resolveIds(relationIds,sInfo);
    }
    // Predicates
    for (auto& pInfo : predicates) {
        resolveIds(relationIds,pInfo.left);
        resolveIds(relationIds,pInfo.right);
    }
    // Filters
    for (auto& fInfo : filters) {
        resolveIds(relationIds,fInfo.filterColumn);
    }
}
//---------------------------------------------------------------------------
void QueryInfo::parseQuery(string& rawQuery)
// Parse query [RELATIONS]|[PREDICATES]|[SELECTS]
{
    clear();
    vector<string> queryParts;
    splitString(rawQuery,queryParts,'|');
    assert(queryParts.size()==3);
    parseRelationIds(queryParts[0]);
    parsePredicates(queryParts[1]);
    parseSelections(queryParts[2]);
    resolveRelationIds();
}
//---------------------------------------------------------------------------
void QueryInfo::clear()
// Reset query info
{
    relationIds.clear();
    predicates.clear();
    filters.clear();
    selections.clear();
}
//---------------------------------------------------------------------------
static string wrapRelationName(uint64_t id)
// Wraps relation id into quotes to be a SQL compliant string
{
    return "\""+to_string(id)+"\"";
}
//---------------------------------------------------------------------------
string SelectInfo::dumpSQL(bool addSUM)
// Appends a selection info to the stream
{
    auto innerPart=wrapRelationName(binding)+".c"+to_string(colId);
    return addSUM?"SUM("+innerPart+")":innerPart;
}
//---------------------------------------------------------------------------
string SelectInfo::dumpText()
// Dump text format
{
    return to_string(binding)+"."+to_string(colId);
}
//--------------------------------------------------------------------------
string SelectInfo::dumpLabel() const
// Dump the graph label in text format
{
    return to_string(relId)+"."+to_string(binding)+"."+to_string(colId);
}
//---------------------------------------------------------------------------
string FilterInfo::dumpText()
// Dump text format
{
    return filterColumn.dumpText()+static_cast<char>(comparison)+to_string(constant);
}
//---------------------------------------------------------------------------
string FilterInfo::dumpLabel() const
// Dump the graph label in text format
{
    return filterColumn.dumpLabel()+static_cast<char>(comparison)+to_string(constant);
}
//---------------------------------------------------------------------------
string FilterInfo::dumpSQL()
// Dump text format
{
    return filterColumn.dumpSQL()+static_cast<char>(comparison)+to_string(constant);
}
//---------------------------------------------------------------------------
string PredicateInfo::dumpText()
// Dump text format
{
    return left.dumpText()+'='+right.dumpText();
}
//---------------------------------------------------------------------------
string PredicateInfo::dumpLabel() const
// Dump the graph label in text format
{
    return left.dumpLabel()+'='+right.dumpLabel();
}
//---------------------------------------------------------------------------
string PredicateInfo::dumpSQL()
    // Dump text format
{
    return left.dumpSQL()+'='+right.dumpSQL();
}
//---------------------------------------------------------------------------
template <typename T>
static void dumpPart(stringstream& ss,vector<T> elements)
{
    for (unsigned i=0;i<elements.size();++i) {
        ss << elements[i].dumpText();
        if (i<elements.size()-1)
            ss << T::delimiter;
    }
}
//---------------------------------------------------------------------------
template <typename T>
static void dumpPartSQL(stringstream& ss,vector<T> elements)
{
    for (unsigned i=0;i<elements.size();++i) {
        ss << elements[i].dumpSQL();
        if (i<elements.size()-1)
            ss << T::delimiterSQL;
    }
}
//---------------------------------------------------------------------------
string QueryInfo::dumpText()
// Dump text format
{
    stringstream text;
    // Relations
    for (unsigned i=0;i<relationIds.size();++i) {
        text << relationIds[i];
        if (i<relationIds.size()-1)
            text << " ";
    }
    text << "|";

    dumpPart(text,predicates);
    if (predicates.size()&&filters.size())
        text << PredicateInfo::delimiter;
    dumpPart(text,filters);
    text << "|";
    dumpPart(text,selections);

    return text.str();
}
//---------------------------------------------------------------------------
string QueryInfo::dumpSQL()
// Dump SQL
{
    stringstream sql;
    sql << "SELECT ";
    for (unsigned i=0;i<selections.size();++i) {
        sql << selections[i].dumpSQL(true);
        if (i<selections.size()-1)
            sql << ", ";
    }

    sql << " FROM ";
    for (unsigned i=0;i<relationIds.size();++i) {
        sql << "r" << relationIds[i] << " " << wrapRelationName(i);
        if (i<relationIds.size()-1)
            sql << ", ";
    }

    sql << " WHERE ";
    dumpPartSQL(sql,predicates);
    if (predicates.size()&&filters.size())
        sql << " and ";
    dumpPartSQL(sql,filters);

    sql << ";";

    return sql.str();
}
//---------------------------------------------------------------------------
void QueryInfo::getSelectionsMap(unordered_map<SelectInfo, unsigned> &selectionsMap) const
{
    vector<PredicateInfo>::const_iterator pt;
    for (pt = this->predicates.begin(); pt != this->predicates.end(); ++pt) {
        try {
            selectionsMap.at(pt->left) += 1;
        }
        catch (const out_of_range &) {
            selectionsMap[pt->left] = 1;
        }

        try {
            selectionsMap.at(pt->right) += 1;
        }
        catch (const out_of_range &) {
            selectionsMap[pt->right] = 1;
        }
    }

    vector<FilterInfo>::const_iterator ft;
    for (ft = this->filters.begin(); ft != this->filters.end(); ++ft) {
        try {
            selectionsMap.at(ft->filterColumn) += 1;
        }
        catch (const out_of_range &) {
            selectionsMap[ft->filterColumn] = 1;
        }
    }

    vector<SelectInfo>::const_iterator st;
    for (st = this->selections.begin(); st != this->selections.end(); ++st) {
        try {
            selectionsMap.at((*st)) += 1;
        }
        catch (const out_of_range &) {
            selectionsMap[(*st)] = 1;
        }
    }
}
//---------------------------------------------------------------------------
QueryInfo::QueryInfo(string rawQuery) { parseQuery(rawQuery); }
//---------------------------------------------------------------------------
bool SelectInfo::operator==(const SelectInfo& o) const {
    return o.relId == relId && o.binding == binding && o.colId == colId;
}
//---------------------------------------------------------------------------
bool PredicateInfo::operator==(const PredicateInfo& o) const {

    if((this->left).relId == o.left.relId){
        if((this->left).colId == o.left.colId &&
                (this->right).relId == o.right.relId &&
                (this->right).colId == o.right.colId)
        {
            return true;
        }else{
            return false;
        }
    }else if((this->left).relId == o.right.relId){
        if((this->left).colId == o.right.colId &&
           (this->right).relId == o.left.relId &&
           (this->right).colId == o.left.colId)
        {
            return true;
        }else{
            return false;
        }
    }

    return false;
}
//---------------------------------------------------------------------------