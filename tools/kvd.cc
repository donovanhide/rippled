#include <boost/algorithm/hex.hpp>
#include <iostream>
#include <algorithm>
#include <vector>
#include <string>
#include <csignal>
#include <chrono>
#include "db/db.h"

using boost::algorithm::unhex;
using boost::algorithm::hex;
using namespace keyvadb;
using namespace std::chrono;

// This tool is stupidly slow when compiled with libc++
// http://llvm.org/bugs/show_bug.cgi?id=21192
int main()
{
    Options options;
    options.keyFileName = "kvd.keys";
    options.valueFileName = "kvd.values";
    DB<256, StandardLog> db(options);
    if (auto err = db.Open())
    {
        std::cerr << err.message() << std::endl;
        return 1;
    }
    if (auto err = db.Clear())
    {
        std::cerr << err.message() << std::endl;
        return 1;
    }
    std::vector<std::string> inserted;
    std::ios_base::sync_with_stdio(false);
    auto start = high_resolution_clock::now();
    std::string line;
    while (std::getline(std::cin, line))
    {
        if (line.find(':') != 64)
            throw std::invalid_argument("bad line format");
        auto key = unhex(line.substr(0, 64));
        auto value = unhex(line.substr(65, std::string::npos));
        if (auto err = db.Put(key, value))
        {
            std::cout << err.message() << std::endl;
        }
        inserted.push_back(key);
    }

    auto finish = high_resolution_clock::now();
    auto dur = duration_cast<nanoseconds>(finish - start);
    std::cout << "Puts: " << dur.count() / inserted.size() << " ns/key"
              << std::endl;
    start = high_resolution_clock::now();
    std::string value;
    for (auto const& key : inserted)
        if (auto err = db.Get(key, &value))
            std::cout << hex(key) << ":" << err.message() << std::endl;
    finish = high_resolution_clock::now();
    dur = duration_cast<nanoseconds>(finish - start);
    std::cout << "Gets: " << dur.count() / inserted.size() << " ns/key"
              << std::endl;
    return 0;
}
