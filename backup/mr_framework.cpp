#include "mr_framework.h"
#include "debug.h"
#include <cassert>
#include <filesystem>
#include <numeric>
#include <list>
#include <string>
#include <string_view>
#include <sstream>
#include <fstream>
// ------------------------------------------------
// [[[  citem_t friend operators implementation...

std::ofstream &operator<<(std::ofstream &os, const citem_t &it)
{
    try
    {
        os << it.key << " " << it.val;
    }
    catch (std::ofstream::failure &e)
    {
        std::cerr << e.what() << '\n';
    }
    return os;
}

std::ifstream &operator>>(std::ifstream &is, citem_t &it)
{
    try
    {
        is >> it.key >> it.val;
    }
    catch (std::ifstream::failure &e)
    {
        std::cerr << e.what() << '\n';
    }
    return is;
}

void basic_sortf_t::operator()(int container_id, pless_t less)
{
    std::vector<citem_t> vec;
    {
        std::ifstream in(workfile_path(container_id));
        while (!in.eof())
        {
            citem_t it;
            in >> it;
            if (it.key.size())
                vec.push_back(it);
        }
    }
    mr_delete_container_file(container_id);

    std::sort(vec.begin(), vec.end(), less);

    std::ofstream out(workfile_path(container_id));
    for (auto it = vec.begin(); it != vec.end(); ++it)
        out << *it << '\n';
}

void mr_shuffle(int mnum, int rnum)
{
    auto inp_containers = make_containers_pool<std::ifstream>(mnum);
    auto out_containers = make_containers_pool<std::ofstream>(rnum, mnum);

    long int out_container_size = i_ceiling(total.load(), static_cast<long>(rnum));

    //
    auto eq_to_prev = false;

    std::vector<std::pair<citem_t, std::ifstream *>> workset; // elements in comparison
    auto out_it = out_containers.begin();

    // Preliminarily fill up the workset[]
    uint i = 0;
    for (auto it = inp_containers.begin();
         i < inp_containers.size(); ++i, ++it)
    {
        citem_t inp_item;
        *it >> inp_item;
        if (it->eof() || !inp_item.key.size())
            continue;
        workset.push_back({inp_item, &(*it)});
    }

    // Fill up output containers
    std::string prev_key{""};
    ulong saved = 0;
    long out_count = 0;
    while (inp_containers.size())
    {

        // Find the minimal element of workset
        auto cur = std::min_element(workset.begin(),
                                    workset.end(), [](std::pair<citem_t, std::ifstream *> a, std::pair<citem_t, std::ifstream *> b)
                                    { return a.first.key < b.first.key; });

        // Is it equal to the previously output element?
        eq_to_prev = (cur->first.key == prev_key);
        prev_key = cur->first.key;

        // If current output container is filled up and the current item != previous item
        // then pass to the next output container
        if (out_count >= out_container_size && !eq_to_prev)
        {
            out_count = 0;
            ++out_it;
        }

        // Output current item and delete it from workset
        (*out_it) << cur->first << '\n';
        out_count++;
        saved++;

        auto p_cont = cur->second;
        workset.erase(cur);

        // Replenish workset from curr input container if the container is not empty
        if (p_cont->eof())
        {
            auto it = get_container_iterator(p_cont, inp_containers);
            inp_containers.erase(it);
            continue;
        }

        citem_t inp_item;
        *p_cont >> inp_item;
        if (!inp_item.key.size())
        {
            auto it = get_container_iterator(p_cont, inp_containers);
            inp_containers.erase(it);
            continue;
        }
        workset.push_back({inp_item, p_cont});
    }
    for (int i = 0; i < mnum; ++i)
    {
        mr_delete_container_file(i);
    }
    mr_normalize_container_names();
}

std::vector<long> mr_split_file(char input_delimiter, int mnum)
{
    std::vector<long> input_vec;
    input_vec.push_back(0);

    try
    {
        auto path = workfile_path(input_file_id);
        auto fsize = std::filesystem::file_size(path);
        std::ifstream f{path};

        unsigned long chunk = i_ceiling(fsize, static_cast<unsigned long>(mnum));

        for (uintmax_t pos = chunk - 1; true; pos += chunk)
        {
            for (uintmax_t shift = 0; true; ++shift)
            {
                if (pos + shift >= fsize)
                {
                    input_vec.push_back(no_pos);
                    return input_vec;
                }

                f.seekg(pos + shift, std::ios_base::beg);
                if (f.peek() == input_delimiter)
                {
                    input_vec.push_back(f.tellg() + static_cast<std::basic_istream<char>::pos_type>(1));
                    break;
                }
            }
        }
        input_vec.push_back(no_pos);
        return input_vec;
    }
    catch (std::ifstream::failure &e)
    {
        std::cerr << e.what() << '\n';
    }
    return input_vec;
}

void mr_create_or_clean_directory(std::string directory)
{
    using namespace std::filesystem;

    if (!exists(directory))
    {
        [[maybe_unused]] auto res = create_directory(directory);
        assert(res);
    }

    const std::filesystem::directory_iterator _end;
    for (std::filesystem::directory_iterator it(directory); it != _end; ++it)
    {
        std::string s = it->path();
        if (s.find("-1") == s.npos)
            std::filesystem::remove(it->path());
    }
}

void mr_init()
{
    mr_create_or_clean_directory(std::string(output_dir));
}

template <typename ConT>
std::list<ConT> make_containers_pool(int nof_items, int shift)
{
    std::list<ConT> containers;
    for (long i = shift; i < (nof_items + shift); ++i)
        containers.emplace_back(workfile_path(i));
    return containers;
}

template <typename PositiveInt>
PositiveInt i_ceiling(PositiveInt numerator, PositiveInt denominator)
{
    return (numerator + denominator - 1) / denominator;
}

std::string workfile_path(int thread_id)
{
    return std::string(output_dir) + "c" + std::to_string(thread_id);
}

void mr_delete_container_file(int thread_id)
{
    using namespace std::filesystem;
    auto path = workfile_path(thread_id);
    if (exists(path))
        remove(path);
};

void rename_files(int base_num, std::string directory)
{
    using namespace std::filesystem;
    const directory_iterator _end;
    for (std::filesystem::directory_iterator it(directory); it != _end; ++it)
    {
        if (std::string(it->path().filename()) == std::string(input_file_name))
            continue;
        std::filesystem::rename(it->path(),
                                std::string(it->path().parent_path()) + "/c" + std::to_string(base_num++));
    }
}

int get_file_id(std::filesystem::path path)
{
    std::istringstream is(path.filename());
    is.get();
    int id;
    is >> id;
    return id;
}

void mr_normalize_container_names()
{
    using namespace std::filesystem;
    auto directory = std::string(output_dir);
    const directory_iterator _end;
    int count = 0;
    int max_id = 0;
    for (std::filesystem::directory_iterator it(directory); it != _end; ++it, ++count)
    {
        auto id = get_file_id(it->path());
        if (id > max_id)
            max_id = id;
    }

    if (max_id == (count - 2))
        return;

    rename_files(max_id + 1, directory);
    rename_files(0, directory);
}

std::list<std::ifstream>::const_iterator
get_container_iterator(const std::ifstream *p_con,
                       const std::list<std::ifstream> &containers)
{
    for (auto it = containers.begin(); it != containers.end(); ++it)
    {
        if (&(*it) == p_con)
            return it;
    }
    return containers.end();
}

// ... Auxillaries]]]
// ----------------------------------------
