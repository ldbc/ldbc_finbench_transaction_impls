#include <iostream>
#include <sstream>

inline int16_t ReadInt16(std::stringstream& iss) {
    int16_t i;
    iss.read((char*)&i, sizeof(int16_t));
    return i;
}

inline int32_t ReadInt32(std::stringstream& iss) {
    int32_t i;
    iss.read((char*)&i, sizeof(int32_t));
    return i;
}

inline int64_t ReadInt64(std::stringstream& iss) {
    int64_t i;
    iss.read((char*)&i, sizeof(int64_t));
    return i;
}

inline std::string ReadString(std::stringstream& iss) {
    int16_t len = ReadInt16(iss);
    std::string s;
    s.resize(len);
    iss.read((char*)s.data(), s.size());
    return std::move(s);
}

inline void WriteInt8(std::stringstream& oss, int8_t i) { oss.write((const char*)&i, sizeof(int8_t)); }

inline void WriteInt16(std::stringstream& oss, int16_t i) { oss.write((const char*)&i, sizeof(int16_t)); }

inline void WriteInt32(std::stringstream& oss, int32_t i) { oss.write((const char*)&i, sizeof(int32_t)); }

inline void WriteInt64(std::stringstream& oss, int64_t i) { oss.write((const char*)&i, sizeof(int64_t)); }

inline void WriteFloat(std::stringstream& oss, float f) { oss.write((const char*)&f, sizeof(float)); }

inline void WriteDouble(std::stringstream& oss, double d) { oss.write((const char*)&d, sizeof(double)); }

inline void WriteString(std::stringstream& oss, const std::string& s) {
    WriteInt16(oss, s.size());
    oss.write((const char*)s.data(), s.size());
}

inline void WriteBool(std::stringstream& oss, bool b) { oss.write((const char*)&b, sizeof(bool)); }

#include <tuple>
#include "date/date.h"

inline std::tuple<int32_t, int32_t, int32_t> GetYearMonthDay(int64_t ts) {
    auto tp = std::chrono::system_clock::time_point(std::chrono::seconds(ts / 1000));
    auto dp = date::floor<date::days>(tp);
    auto ymd = date::year_month_day(dp);
    int32_t year = (int)ymd.year();
    int32_t month = (unsigned)ymd.month();
    int32_t day = (unsigned)ymd.day();
    return std::make_tuple(year, month, day);
}

inline std::pair<int32_t, int32_t> GetYearMonth(int64_t ts) {
    auto tp = std::chrono::system_clock::time_point(std::chrono::seconds(ts / 1000));
    auto dp = date::floor<date::days>(tp);
    auto ymd = date::year_month_day(dp);
    int32_t year = (int)ymd.year();
    int32_t month = (unsigned)ymd.month();
    return std::make_pair(year, month);
}

inline std::pair<int32_t, int32_t> GetMonthDay(int64_t ts) {
    auto tp = std::chrono::system_clock::time_point(std::chrono::seconds(ts / 1000));
    auto dp = date::floor<date::days>(tp);
    auto ymd = date::year_month_day(dp);
    int32_t month = (unsigned)ymd.month();
    int32_t day = (unsigned)ymd.day();
    return std::make_pair(month, day);
}

inline int32_t GetYear(int64_t ts) {
    auto tp = std::chrono::system_clock::time_point(std::chrono::seconds(ts / 1000));
    auto dp = date::floor<date::days>(tp);
    auto ymd = date::year_month_day(dp);
    int32_t year = (int)ymd.year();
    return year;
}

inline int32_t GetMonth(int64_t ts) {
    auto tp = std::chrono::system_clock::time_point(std::chrono::seconds(ts / 1000));
    auto dp = date::floor<date::days>(tp);
    auto ymd = date::year_month_day(dp);
    int32_t month = (unsigned)ymd.month();
    return month;
}

#include <type_traits>
#include "lgraph/lgraph.h"

namespace lgraph_api {

template <class EIT>
class LabeledEdgeIterator : public EIT {
    uint16_t lid_;
    bool valid_;

   public:
    LabeledEdgeIterator(EIT&& eit, uint16_t lid) : EIT(std::move(eit)), lid_(lid) {
        valid_ = EIT::IsValid() && EIT::GetLabelId() == lid_;
    }

    bool IsValid() { return valid_; }

    bool Next() {
        if (!valid_) return false;
        valid_ = (EIT::Next() && EIT::GetLabelId() == lid_);
        return valid_;
    }

    void Reset(VertexIterator& vit, uint16_t lid) { Reset(vit.GetId(), lid); }

    void Reset(size_t vid, uint16_t lid, int64_t tid = 0) {
        lid_ = lid;
        if (std::is_same<EIT, OutEdgeIterator>::value) {
            EIT::Goto(EdgeUid(vid, 0, lid, tid, 0), true);
        } else {
            EIT::Goto(EdgeUid(0, vid, lid, tid, 0), true);
        }
        valid_ = (EIT::IsValid() && EIT::GetLabelId() == lid_);
    }
};

static LabeledEdgeIterator<OutEdgeIterator> LabeledOutEdgeIterator(VertexIterator& vit, uint16_t lid, int64_t tid = 0) {
    return LabeledEdgeIterator<OutEdgeIterator>(std::move(vit.GetOutEdgeIterator(EdgeUid(0, 0, lid, tid, 0), true)),
                                                lid);
}

static LabeledEdgeIterator<InEdgeIterator> LabeledInEdgeIterator(VertexIterator& vit, uint16_t lid, int64_t tid = 0) {
    return LabeledEdgeIterator<InEdgeIterator>(std::move(vit.GetInEdgeIterator(EdgeUid(0, 0, lid, tid, 0), true)), lid);
}

static LabeledEdgeIterator<OutEdgeIterator> LabeledOutEdgeIterator(Transaction& txn, int64_t vid, uint16_t lid,
                                                                   int64_t tid = 0) {
    return LabeledEdgeIterator<OutEdgeIterator>(std::move(txn.GetOutEdgeIterator(EdgeUid(vid, 0, lid, tid, 0), true)),
                                                lid);
}

static LabeledEdgeIterator<InEdgeIterator> LabeledInEdgeIterator(Transaction& txn, int64_t vid, uint16_t lid,
                                                                 int64_t tid = 0) {
    return LabeledEdgeIterator<InEdgeIterator>(std::move(txn.GetInEdgeIterator(EdgeUid(0, vid, lid, tid, 0), true)),
                                               lid);
}

}  // namespace lgraph_api
