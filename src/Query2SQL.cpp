#include <iostream>
#include <vector>
#include "DataEngine.hpp"
#include "Relation.hpp"
#include "Utils.hpp"
#include "Parser.hpp"
//---------------------------------------------------------------------------
std::vector<Relation> DataEngine::relations;
HistCatalog DataEngine::histograms;
//---------------------------------------------------------------------------
int main (void)
{
  std::cout << "Transforms our query format to SQL" << std::endl;

  QueryInfo i;
  for (std::string line; std::getline(std::cin, line);) {
    i.parseQuery(line);
    std::cout << i.dumpSQL() << std::endl;
  }

  return 0;
}
//---------------------------------------------------------------------------
