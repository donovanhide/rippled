#include <boost/algorithm/hex.hpp>
#include <iostream>
#include <iterator>
#include <vector>
#include <fstream>
#include <cstring>
#include <string>
#include <array>

using boost::algorithm::hex;

int main(int argc, char* argv[])
{
    std::ifstream in(argv[1], std::ios::in | std::ios::binary);
    std::uint32_t length;
    std::vector<char> key(32);
    std::vector<char> value;

    while (in)
    {
        in.read(reinterpret_cast<char*>(&length), sizeof(length));
        in.read(key.data(), key.size());
        value.resize(length - key.size() - sizeof(length));
        in.read(value.data(), value.size());
        std::cout << length << ":" << hex(std::string(key.begin(), key.end()))
                  << ":" << hex(std::string(value.begin(), value.end()))
                  << std::endl;
    }
    return 0;
}
