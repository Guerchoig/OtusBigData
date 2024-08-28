#include "mr_framework.h"
#include "debug.h"
#include <vector>
#include <utility>
#include <fstream>

constexpr char input_delimiter = '\n';

struct transformer_t : map_t
{
    citem_t operator()(std::ifstream &ins) override
    {
        std::string s;
        std::getline(ins, s);
        return citem_t{s, 0};
    }
};

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
    int mnum, rnum;
    if (!get_params(argc, argv, mnum, rnum))
        return 0;

    mr_init();

    auto boundaries = mr_split_file(input_delimiter, mnum);
    {
        mr_stage_t<transformer_t> map(mnum, boundaries, &mr_sort);
    }
    mr_shuffle(mnum, rnum);
    {
        mr_stage_t<accumulator_t> reduce_1(rnum, {});
    }
    mr_shuffle(rnum, 1);
    {
        mr_stage_t<maximizer_t> reduce_2(1, {});
    }
    return 0;
}
