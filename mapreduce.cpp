/**
 * @brief mapreduce.cpp
 * A sample client to test file-based map-reduce framework
 */
#include "mr_framework.h"
#include "debug.h"
#include <vector>
#include <utility>
#include <fstream>

constexpr char input_delimiter = '\n';

/**
 * @brief Transformer object for first map stage
 */
struct transformer_t : map_t
{
    citem_t operator()(std::ifstream &ins) override
    {
        std::string s;
        std::getline(ins, s);
        return citem_t{s, 0};
    }
};

/**
 * @brief Accumulator object for first reduce stage
 */
struct accumulator_t : reduce_t
{
    citem_t operator()(std::ifstream &_in) override final
    {
        citem_t it;
        _in >> it;
        if (!it.key.size())
            return result;
        long maxlen;
        for (maxlen = result.val; maxlen < static_cast<long>(it.key.size()); maxlen++)
        {
            if (result.key.substr(0, maxlen) != it.key.substr(0, maxlen))
                break;
        }

        result.key = it.key;
        if (result.val < maxlen)
            result.val = maxlen;

        return result;
    }
    accumulator_t()
    {
        result = {"", 1};
    }
    accumulator_t([[maybe_unused]] accumulator_t *p)
    {
        result = {"", 1};
    }
};

/**
 * @brief Accumulator object for final reduce stage
 */
struct maximizer_t : reduce_t
{
    citem_t operator()(std::ifstream &_in) override final
    {
        citem_t it;
        _in >> it;
        if (!it.key.size())
            return result;
        result.val = std::max(result.val, it.val);
        return result;
    }
    maximizer_t()
    {
        result = {"", 0};
    }
};

int main(int argc, char **argv)
{
    // Get args
    int mnum, rnum;
    if (!get_params(argc, argv, mnum, rnum))
        return 0;

    // Init the framework
    mr_init();

    // Find mnum + 1 boundaries of the input file, named "c-1"
    auto boundaries = mr_split_file(input_delimiter, mnum);

    // Produce mnum files ("c0 - c<mnum-1>") from the input file
    {
        mr_stage_t<transformer_t> map(mnum, boundaries, &mr_sort);
    }

    // Shuffle results into rnum files ("c0 - c<rnum-1>")
    mr_shuffle(mnum, rnum);

    // Find max prefix length for every file
    {
        mr_stage_t<accumulator_t> reduce_1(rnum, {});
    }

    // Shuffle results into a single file "c0"
    mr_shuffle(rnum, 1);

    // Find maximum prefix in a single file and output it into "c0"
    {
        mr_stage_t<maximizer_t> reduce_2(1, {});
    }
    return 0;
}
