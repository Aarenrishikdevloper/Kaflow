#include "../src/protocol/compact_reader.hpp"
#include "../src/protocol/compact_writer.hpp"

#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

int main()
{
    // Create a compact writer.
    CompacrWriter writer;

    // Encode the string "cat".
    writer.write_compact_string("cat");

    // Print every encoded byte in hexadecimal.
    for (std::byte value : writer.data()) {
        std::printf(
            "%02X ",
            std::to_integer<unsigned int>(value)
        );
    }

    std::printf("\n");

    // Copy the encoded bytes into a request buffer.
    const std::vector<std::byte> encoded = writer.data();

    // Create a reader for those bytes.
    CompactReader reader(encoded);

    // Decode the compact string.
    const std::string decoded = reader.read_compact_string();

    // Print the decoded value.
    std::cout << "Decoded: " << decoded << '\n';

    return 0;
}