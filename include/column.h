#pragma once

#include <vector>
#include <optional>
#include <typeindex>


struct ColumnBase
{
  virtual ~ColumnBase() = default;
  virtual size_t size() const = 0;
  virtual std::type_index get_type() const = 0;
};

template <typename T>
class Column : public ColumnBase 
{
    private:
    std::vector<std::optional<T>> data;
    std::type_index type_info{typeid(T)};

  public:
    Column(const std::vector<std::optional<T>>& d) : data(d) {};

    using iterator = typename std::vector<std::optional<T>>::iterator;
    using const_iterator = typename std::vector<std::optional<T>>::const_iterator;

    std::type_index get_type() const override
    {
      return type_info;
    }

    void append(std::optional<T> value)
    {
      data.push_back(value);
    }

    size_t size() const override
    {
      return data.size();
    }

    bool empty() const
    {
      return data.empty();
    }

    std::optional<T>& operator[](size_t i)
    {
      if (i >= data.size())
      {
        throw std::out_of_range("column index out of range");
      }
      
      return data[i];
    }

    const std::optional<T>& operator[](size_t i) const
    {
      if (i >= data.size())
      {
        throw std::out_of_range("column index out of range");
      }

      return data[i];
    }

    iterator begin()
    {
      return data.begin();
    }

    const_iterator begin() const
    {
      return data.begin();
    }

    const_iterator cbegin() const noexcept
    {
      return data.cbegin();
    }

    iterator end()
    {
      return data.end();
    }

    const_iterator end() const
    {
      return data.end();
    }

    const_iterator cend() const noexcept
    {
      return data.cend();
    }

    std::optional<T>& front()
    {
      return data.front();
    }

    const std::optional<T>& front() const
    {
      return data.front();
    }

    std::optional<T>& back()
    {
      return data.back();
    }
  
    const std::optional<T>& back() const
    {
      return data.back();
    }
};
