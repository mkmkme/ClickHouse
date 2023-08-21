#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

enum class ShiftRotateStrategy : uint8_t
{
    Shift,
    Rotate
};

enum class ShiftRotateDirection : uint8_t
{
    Left,
    Right
};

/// A helper structure for storing either a constant value or a full column and working with it.
/// Useful for dealing with 'shift number' and 'default value' columns.
struct ConstOrColumn
{
    ConstOrColumn() = default;
    explicit ConstOrColumn(ColumnPtr src) { init(src); }

    void init(ColumnPtr src)
    {
        if (isColumnConst(*src))
            const_value = (*src)[0];
        else
            column = src;
    }

    Int64 getInt(size_t idx) const
    {
        if (column)
            return column->getInt(idx);
        else
            return const_value.get<Int64>();
    }

    void insertManyToColumn(IColumn & dst, size_t row_num, size_t length) const
    {
        if (column)
            dst.insertManyFrom(*column, row_num, length);
        else
            dst.insertMany(const_value, length);
    }

    Field const_value;
    ColumnPtr column;
};

template <typename Impl, typename Name>
class FunctionArrayShiftRotate : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static constexpr ShiftRotateStrategy strategy = Impl::strategy;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayShiftRotate>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return strategy == ShiftRotateStrategy::Shift; }
    size_t getNumberOfArguments() const override { return strategy == ShiftRotateStrategy::Rotate ? 2 : 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if constexpr (strategy == ShiftRotateStrategy::Shift)
        {
            if (arguments.size() < 2)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least two arguments.", getName());

            if (arguments.size() > 3)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at most three arguments.", getName());
        }

        const DataTypePtr & first_arg = arguments[0];
        if (!isArray(first_arg))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}, expected Array",
                arguments[0]->getName(),
                getName());

        if (!isNativeInteger(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}, expected Native Integer",
                arguments[1]->getName(),
                getName());

        const DataTypePtr & elem_type = static_cast<const DataTypeArray &>(*first_arg).getNestedType();
        if (arguments.size() == 3)
        {
            auto ret = tryGetLeastSupertype(DataTypes{elem_type, arguments[2]});
            // Note that this will fail if the default value does not fit into the array element type (e.g. UInt64 and Array(UInt8)).
            // In this case array should be converted to Array(UInt64) explicitly.
            if (!ret || !ret->equals(*elem_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of function {}, expected {}",
                    arguments[2]->getName(),
                    getName(),
                    elem_type->getName());
        }

        return std::make_shared<DataTypeArray>(elem_type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnPtr column_array_ptr = arguments[0].column;
        const auto * column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());

        if (!column_array)
        {
            const auto * column_const_array = checkAndGetColumnConst<ColumnArray>(column_array_ptr.get());
            if (!column_const_array)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected Array column, found {}", column_array_ptr->getName());

            column_array_ptr = column_const_array->convertToFullColumn();
            column_array = assert_cast<const ColumnArray *>(column_array_ptr.get());
        }

        ConstOrColumn shift_num_const_or_column(arguments[1].column);

        if constexpr (strategy == ShiftRotateStrategy::Shift)
        {
            ConstOrColumn default_const_or_column{};
            const auto elem_type = static_cast<const DataTypeArray &>(*result_type).getNestedType();

            if (arguments.size() == 3)
                default_const_or_column.init(arguments[2].column);
            else
                default_const_or_column.const_value = elem_type->getDefault();

            return Impl::execute(*column_array, std::move(shift_num_const_or_column), std::move(default_const_or_column), input_rows_count);
        }
        else
        {
            return Impl::execute(*column_array, std::move(shift_num_const_or_column), input_rows_count);
        }
    }
};

template <ShiftRotateDirection direction>
struct ArrayRotateImpl
{
    static constexpr ShiftRotateStrategy strategy = ShiftRotateStrategy::Rotate;
    static ColumnPtr execute(const ColumnArray & array, ConstOrColumn shift_num, size_t input_rows_count)
    {
        size_t batch_size = array.getData().size();

        IColumn::Permutation permutation(batch_size);
        const IColumn::Offsets & offsets = array.getOffsets();

        IColumn::Offset current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const size_t offset = offsets[i];
            const size_t nested_size = offset - current_offset;
            Int64 shift_num_value = shift_num.getInt(i);

            // Rotating left to -N is the same as rotating right to N.
            ShiftRotateDirection actual_direction = direction;
            if (shift_num_value < 0)
            {
                actual_direction = (direction == ShiftRotateDirection::Left) ? ShiftRotateDirection::Right : ShiftRotateDirection::Left;
                shift_num_value = -shift_num_value;
            }

            shift_num_value %= nested_size;

            // Rotating left to N is the same as shifting right to (size - N).
            if (actual_direction == ShiftRotateDirection::Right)
                shift_num_value = nested_size - shift_num_value;

            for (size_t j = 0; j < nested_size; ++j)
                permutation[current_offset + j] = current_offset + (j + shift_num_value) % nested_size;

            current_offset = offset;
        }

        return ColumnArray::create(array.getData().permute(permutation, 0), array.getOffsetsPtr());
    }
};

template <ShiftRotateDirection direction>
struct ArrayShiftImpl
{
    static constexpr ShiftRotateStrategy strategy = ShiftRotateStrategy::Shift;

    static ColumnPtr
    execute(const ColumnArray & array, ConstOrColumn shift_const_or_column, ConstOrColumn default_const_or_column, size_t input_column_rows)
    {
        const IColumn::Offsets & offsets = array.getOffsets();
        const IColumn & array_data = array.getData();
        const size_t data_size = array_data.size();

        auto result_column = array.getData().cloneEmpty();
        result_column->reserve(data_size);

        IColumn::Offset current_offset = 0;
        for (size_t i = 0; i < input_column_rows; ++i)
        {
            const size_t offset = offsets[i];
            const size_t nested_size = offset - current_offset;
            Int64 shift_num_value = shift_const_or_column.getInt(i);

            // Shifting left to -N is the same as shifting right to N.
            ShiftRotateDirection actual_direction = direction;
            if (shift_num_value < 0)
            {
                actual_direction = (direction == ShiftRotateDirection::Left) ? ShiftRotateDirection::Right : ShiftRotateDirection::Left;
                shift_num_value = -shift_num_value;
            }

            const size_t number_of_default_values = std::min(static_cast<size_t>(shift_num_value), nested_size);
            const size_t num_of_original_values = nested_size - number_of_default_values;

            if (actual_direction == ShiftRotateDirection::Right)
            {
                default_const_or_column.insertManyToColumn(*result_column, i, number_of_default_values);
                result_column->insertRangeFrom(array_data, current_offset, num_of_original_values);
            }
            else
            {
                result_column->insertRangeFrom(array_data, current_offset + number_of_default_values, num_of_original_values);
                default_const_or_column.insertManyToColumn(*result_column, i, number_of_default_values);
            }

            current_offset = offset;
        }

        return ColumnArray::create(std::move(result_column), array.getOffsetsPtr());
    }
};

struct NameArrayShiftLeft
{
    static constexpr auto name = "arrayShiftLeft";
};

struct NameArrayShiftRight
{
    static constexpr auto name = "arrayShiftRight";
};

struct NameArrayRotateLeft
{
    static constexpr auto name = "arrayRotateLeft";
};

struct NameArrayRotateRight
{
    static constexpr auto name = "arrayRotateRight";
};

using ArrayShiftLeftImpl = ArrayShiftImpl<ShiftRotateDirection::Left>;
using FunctionArrayShiftLeft = FunctionArrayShiftRotate<ArrayShiftLeftImpl, NameArrayShiftLeft>;

using ArrayShiftRightImpl = ArrayShiftImpl<ShiftRotateDirection::Right>;
using FunctionArrayShiftRight = FunctionArrayShiftRotate<ArrayShiftRightImpl, NameArrayShiftRight>;

using ArrayRotateLeftImpl = ArrayRotateImpl<ShiftRotateDirection::Left>;
using FunctionArrayRotateLeft = FunctionArrayShiftRotate<ArrayRotateLeftImpl, NameArrayRotateLeft>;

using ArrayRotateRightImpl = ArrayRotateImpl<ShiftRotateDirection::Right>;
using FunctionArrayRotateRight = FunctionArrayShiftRotate<ArrayRotateRightImpl, NameArrayRotateRight>;


REGISTER_FUNCTION(ArrayShiftOrRotate)
{
    factory.registerFunction<FunctionArrayRotateLeft>(
        FunctionDocumentation{
        .description = R"(
Returns an array of the same size as the original array with elements rotated
to the left by the specified number of positions.
[example:simple_int]
[example:overflow_int]
[example:simple_string]
[example:simple_array]
[example:simple_nested_array]

Negative rotate values are treated as rotating to the right by the absolute
value of the rotation.
[example:negative_rotation_int]
)",
        .examples{
            {"simple_int", "SELECT arrayRotateLeft([1, 2, 3, 4, 5], 3)", "[4, 5, 1, 2, 3]"},
            {"simple_string", "SELECT arrayRotateLeft(['a', 'b', 'c', 'd', 'e'], 3)", "['d', 'e', 'a', 'b', 'c']"},
            {"simple_array", "SELECT arrayRotateLeft([[1, 2], [3, 4], [5, 6]], 2)", "[[5, 6], [1, 2], [3, 4]]"},
            {"simple_nested_array",
             "SELECT arrayRotateLeft([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], 1)",
             "[[[5, 6], [7, 8]], [[1, 2], [3, 4]]]"},
            {"negative_rotation_int", "SELECT arrayRotateLeft([1, 2, 3, 4, 5], -3)", "[3, 4, 5, 1, 2]"},
            {"overflow_int", "SELECT arrayRotateLeft([1, 2, 3, 4, 5], 8)", "[4, 5, 1, 2, 3]"},

        },
        .categories = {"Array"},
    });
    factory.registerFunction<FunctionArrayRotateRight>(
        FunctionDocumentation{
        .description = R"(
Returns an array of the same size as the original array with elements rotated
to the right by the specified number of positions.
[example:simple_int]
[example:overflow_int]
[example:simple_string]
[example:simple_array]
[example:simple_nested_array]

Negative rotate values are treated as rotating to the left by the absolute
value of the rotation.
[example:negative_rotation_int]
)",
        .examples{
            {"simple_int", "SELECT arrayRotateRight([1, 2, 3, 4, 5], 3)", "[3, 4, 5, 1, 2]"},
            {"simple_string", "SELECT arrayRotateRight(['a', 'b', 'c', 'd', 'e'], 3)", "['c', 'd', 'e', 'a', 'b']"},
            {"simple_array", "SELECT arrayRotateRight([[1, 2], [3, 4], [5, 6]], 2)", "[[3, 4], [5, 6], [1, 2]]"},
            {"simple_nested_array",
             "SELECT arrayRotateRight([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], 1)",
             "[[[7, 8], [1, 2]], [[3, 4], [5, 6]]]"},
            {"negative_rotation_int", "SELECT arrayRotateRight([1, 2, 3, 4, 5], -3)", "[4, 5, 1, 2, 3]"},
            {"overflow_int", "SELECT arrayRotateRight([1, 2, 3, 4, 5], 8)", "[4, 5, 1, 2, 3]"},
        },
        .categories = {"Array"},
    });
    factory.registerFunction<FunctionArrayShiftLeft>(
        FunctionDocumentation{
        .description = R"(
Returns an array of the same size as the original array with elements shifted
to the left by the specified number of positions. New elements are filled with
provided default values or default values of the corresponding type.
[example:simple_int]
[example:overflow_int]
[example:simple_string]
[example:simple_array]
[example:simple_nested_array]

Negative shift values are treated as shifting to the right by the absolute
value of the shift.
[example:negative_shift_int]

The default value must be of the same type as the array elements.
[example:simple_int_with_default]
[example:simple_string_with_default]
[example:simple_array_with_default]
[example:casted_array_with_default]
)",
        .examples{
            {"simple_int", "SELECT arrayShiftLeft([1, 2, 3, 4, 5], 3)", "[4, 5, 0, 0, 0]"},
            {"negative_shift_int", "SELECT arrayShiftLeft([1, 2, 3, 4, 5], -3)", "[0, 0, 0, 1, 2]"},
            {"overflow_int", "SELECT arrayShiftLeft([1, 2, 3, 4, 5], 8)", "[0, 0, 0, 0, 0]"},
            {"simple_string", "SELECT arrayShiftLeft(['a', 'b', 'c', 'd', 'e'], 3)", "['d', 'e', '', '', '']"},
            {"simple_array", "SELECT arrayShiftLeft([[1, 2], [3, 4], [5, 6]], 2)", "[[5, 6], [], []]"},
            {"simple_nested_array", "SELECT arrayShiftLeft([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], 1)", "[[[5, 6], [7, 8]], []]"},
            {"simple_int_with_default", "SELECT arrayShiftLeft([1, 2, 3, 4, 5], 3, 7)", "[4, 5, 7, 7, 7]"},
            {"simple_string_with_default", "SELECT arrayShiftLeft(['a', 'b', 'c', 'd', 'e'], 3, 'foo')", "['d', 'e', 'foo', 'foo', 'foo']"},
            {"simple_array_with_default", "SELECT arrayShiftLeft([[1, 2], [3, 4], [5, 6]], 2, [7, 8])", "[[5, 6], [7, 8], [7, 8]]"},
            {"casted_array_with_default",
             "SELECT arrayShiftLeft(CAST('[1, 2, 3, 4, 5, 6]', 'Array(UInt16)'), 1, 1000)",
             "[2, 3, 4, 5, 6, 1000]"},
        },
        .categories = {"Array"},
    });
    factory.registerFunction<FunctionArrayShiftRight>(
        FunctionDocumentation{
        .description = R"(
Returns an array of the same size as the original array with elements shifted
to the right by the specified number of positions. New elements are filled with
provided default values or default values of the corresponding type.
[example:simple_int]
[example:overflow_int]
[example:simple_string]
[example:simple_array]
[example:simple_nested_array]

Negative shift values are treated as shifting to the left by the absolute
value of the shift.
[example:negative_shift_int]

The default value must be of the same type as the array elements.
[example:simple_int_with_default]
[example:simple_string_with_default]
[example:simple_array_with_default]
[example:casted_array_with_default]
)",
        .examples{
            {"simple_int", "SELECT arrayShiftRight([1, 2, 3, 4, 5], 3)", "[0, 0, 0, 1, 2]"},
            {"negative_shift_int", "SELECT arrayShiftRight([1, 2, 3, 4, 5], -3)", "[4, 5, 0, 0, 0]"},
            {"overflow_int", "SELECT arrayShiftRight([1, 2, 3, 4, 5], 8)", "[0, 0, 0, 0, 0]"},
            {"simple_string", "SELECT arrayShiftRight(['a', 'b', 'c', 'd', 'e'], 3)", "['', '', '', 'a', 'b']"},
            {"simple_array", "SELECT arrayShiftRight([[1, 2], [3, 4], [5, 6]], 2)", "[[], [], [1, 2]]"},
            {"simple_nested_array", "SELECT arrayShiftRight([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], 1)", "[[], [[1, 2], [3, 4]]]"},
            {"simple_int_with_default", "SELECT arrayShiftRight([1, 2, 3, 4, 5], 3, 7)", "[7, 7, 7, 1, 2]"},
            {"simple_string_with_default",
             "SELECT arrayShiftRight(['a', 'b', 'c', 'd', 'e'], 3, 'foo')",
             "['foo', 'foo', 'foo', 'a', 'b']"},
            {"simple_array_with_default", "SELECT arrayShiftRight([[1, 2], [3, 4], [5, 6]], 2, [7, 8])", "[[7, 8], [7, 8], [1, 2]]"},
            {"casted_array_with_default",
             "SELECT arrayShiftRight(CAST('[1, 2, 3, 4, 5, 6]', 'Array(UInt16)'), 1, 1000)",
             "[1000, 1, 2, 3, 4, 5]"},
        },
        .categories = {"Array"},
    });
}

}
