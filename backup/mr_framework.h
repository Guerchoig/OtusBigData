#pragma once

#include "debug.h"
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>
#include <list>
#include <thread>
#include <fstream>
#include <cstring>
#include <memory>
#include <system_error>

constexpr int input_file_id = -1;
constexpr char input_file_name[] = "c-1";
constexpr long no_pos = -1;
constexpr char output_dir[] = "./output/";

static constexpr bool del_on_destruct = true;
inline std::atomic<long> total;

void mr_delete_container_file(int thread_id);
void mr_normalize_container_names();

template <typename PositiveInt>
PositiveInt i_ceiling(PositiveInt numerator, PositiveInt denominator);

void mr_shuffle(int mnum, int rnum);
std::vector<long> mr_split_file(char input_delimiter, int mnum);
void mr_create_or_clean_directory(std::string directory);
void mr_init();

inline bool get_params(int argc, char **argv, int &mnum, int &rnum)
{
    bool res = true;

    switch (argc)
    {
    case 3:
        mnum = std::atoi(argv[1]);
        rnum = std::atoi(argv[2]);
        break;
    default:
        std::cout << "The use is: mapreduce <mnum> <rnum>\n";
        res = false;
        break;
    }
    return res;
}

std::string workfile_path(int _id);

struct citem_t // container item
{
    std::string key;
    int val = 0;
    friend std::ostream &operator<<(std::ostream &os, const citem_t &it);
    friend std::istream &operator>>(std::istream &os, const citem_t &it);
};
std::ofstream &operator<<(std::ofstream &os, const citem_t &it);
std::ifstream &operator>>(std::ifstream &is, citem_t &it);

using pless_t = bool (*)(const citem_t &a, const citem_t &b); // ptr to 'less' func for citems

inline bool citem_less_key(const citem_t &a, const citem_t &b)
{
    return a.key < b.key;
}

inline bool citem_less_val(const citem_t &a, const citem_t &b)
{
    return a.val < b.val;
}

struct basic_sortf_t
{
    virtual void operator()(int container_id, pless_t less = citem_less_key);
    int dumm;
};
inline basic_sortf_t mr_sort;

struct IFunc
{
    citem_t result;
    virtual citem_t operator()(std::ifstream &in) = 0;
};

struct map_t : IFunc
{
};

struct reduce_t : IFunc
{
};

template <typename T>
void thread_worker(int _inp_id,
                   int _out_id,
                   long int start_pos,
                   long int end_pos,
                   basic_sortf_t *sortf)

{
    {
        std::ifstream ic(workfile_path(_inp_id));
        std::ofstream oc(workfile_path(_out_id));
        ic.seekg(start_pos);
        T mdf;

        if constexpr (std::derived_from<T, map_t>)
        {
            while (!ic.eof() &&
                   (end_pos == no_pos || (end_pos != no_pos && ic.tellg() < end_pos)))
            {
                auto res = mdf(ic);
                total++;
                oc << res << '\n';
            }
        }
        else if constexpr (std::derived_from<T, reduce_t>)
        {
            citem_t res;
            while (!ic.eof() && (end_pos == no_pos || (end_pos != no_pos && ic.tellg() < end_pos)))
            {
                res = mdf(ic);
            }
            oc << res << '\n';
            mr_delete_container_file(_inp_id);
        }
    }
    if constexpr (std::derived_from<T, map_t>)
    {
        if (sortf)
            (*sortf)(_out_id);
    }
}

template <typename T>
struct mr_stage_t
{
    std::list<std::thread> threads;

    // mr_stage_t(int count,
    //                   [[maybe_unused]] const std::vector<long> &input_boundaries,
    //                   basic_sortf_t *sortf = nullptr);
    mr_stage_t(int count,
               const std::vector<long> &input_boundaries,
               basic_sortf_t *sortf = &mr_sort)
    {

        bool splitted_input = (input_boundaries.size() != 0);
        for (int i = 0; i < count; ++i)
        {
            threads.push_back(std::thread(thread_worker<T>,
                                          splitted_input ? input_file_id : i, i + count,
                                          splitted_input ? input_boundaries[i] : 0,
                                          splitted_input ? input_boundaries[i + 1] : no_pos,
                                          sortf));
        }
        for (auto it = threads.begin(); it != threads.end(); ++it)
            it->join();
        mr_normalize_container_names();
    }
};

std::string workfile_path(int _id);

std::list<std::ifstream>::const_iterator
get_container_iterator(const std::ifstream *p_con,
                       const std::list<std::ifstream> &containers);

template <typename ConT>
std::list<ConT> make_containers_pool(int nof_items, int shift = 0);
