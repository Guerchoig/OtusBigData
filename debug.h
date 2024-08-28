/**
 * @brief debug.h
 * contains some declaration for multithread debugging
 */
#pragma once
#include <string>
#include <thread>
#include <vector>

// #define DEBUG

std::string pid_to_string(std::thread *p);
std::string this_pid_to_string();

#ifdef DEBUG
#define _DS(s) std::cout << (this_pid_to_string() + " " + s + "\n");
#define _DF _DS(__PRETTY_FUNCTION__)
#else
#define _DS(s) ;
#define _DF ;
#endif

inline std::string pid_to_string(std::thread *p)
{
    return std::to_string(std::hash<std::thread::id>{}(p->get_id()));
}

inline std::string this_pid_to_string()
{
    return std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()));
}

// #ifdef DEBUG
//     std::ofstream out("out.txt");                // откроем файл для вывод
//     std::streambuf *coutbuf = std::cout.rdbuf(); // запомним старый буфер
//     std::cout.rdbuf(out.rdbuf());                // и теперь все будет в файл out.txt!
//     int i = 0;
//     for (auto it : transformers)
//     {
//         while (true)
//         {
//             auto s = it->get_smallest();
//             if (s.key[0] == mapreduce_t::container_empty)
//                 break;
//             std::cout << i << " " << s.key << "  " << s.val << '\n';
//         }
//         ++i;
//     }
//     std::cout.rdbuf(coutbuf);
// #endif
