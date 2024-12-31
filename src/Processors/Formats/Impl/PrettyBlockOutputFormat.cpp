#include <Processors/Formats/Impl/PrettyBlockOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/PrettyFormatHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/UTF8Helpers.h>
#include <Common/PODArray.h>
#include <Common/formatReadable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

#include <algorithm>


namespace DB
{

PrettyBlockOutputFormat::PrettyBlockOutputFormat(
    WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_, Style style_, bool mono_block_, bool color_)
     : IOutputFormat(header_, out_), format_settings(format_settings_), serializations(header_.getSerializations()), style(style_), mono_block(mono_block_), color(color_)
{
    /// Decide whether we should print a tip near the single number value in the result.
    if (header_.getColumns().size() == 1)
    {
        /// Check if it is a numeric type, possible wrapped by Nullable or LowCardinality.
        DataTypePtr type = removeNullable(recursiveRemoveLowCardinality(header_.getDataTypes().at(0)));
        if (isNumber(type))
            readable_number_tip = true;
    }
}

bool PrettyBlockOutputFormat::cutInTheMiddle(size_t row_num, size_t num_rows, size_t max_rows)
{
    return num_rows > max_rows
        && !(row_num < (max_rows + 1) / 2
            || row_num >= num_rows - max_rows / 2);
}


/// Evaluate the visible width of the values and column names.
/// Note that number of code points is just a rough approximation of visible string width.
void PrettyBlockOutputFormat::calculateWidths(
    const Block & header, const Chunk & chunk,
    WidthsPerColumn & widths, Widths & max_padded_widths, Widths & name_widths, Strings & names)
{
    size_t num_rows = chunk.getNumRows();
    size_t num_displayed_rows = std::min<size_t>(num_rows, format_settings.pretty.max_rows);

    /// len(num_rows + total_rows) + len(". ")
    row_number_width = static_cast<size_t>(std::floor(std::log10(num_rows + total_rows))) + 3;

    size_t num_columns = chunk.getNumColumns();
    const auto & columns = chunk.getColumns();

    widths.resize(num_columns);
    max_padded_widths.resize_fill(num_columns);
    name_widths.resize(num_columns);
    names.resize(num_columns);

    /// Calculate the widths of all values.
    String serialized_value;
    size_t prefix = 2; // Tab character adjustment
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & elem = header.getByPosition(i);
        const auto & column = columns[i];

        widths[i].resize(num_displayed_rows);

        size_t displayed_row = 0;
        for (size_t j = 0; j < num_rows; ++j)
        {
            if (cutInTheMiddle(j, num_rows, format_settings.pretty.max_rows))
                continue;

            {
                WriteBufferFromString out_serialize(serialized_value);
                auto serialization = elem.type->getDefaultSerialization();
                serialization->serializeText(*column, j, out_serialize, format_settings);
            }

            /// Avoid calculating width of too long strings by limiting the size in bytes.
            /// Note that it is just an estimation. 4 is the maximum size of Unicode code point in bytes in UTF-8.
            /// But it's possible that the string is long in bytes but very short in visible size.
            /// (e.g. non-printable characters, diacritics, combining characters)
            if (format_settings.pretty.max_value_width)
            {
                size_t max_byte_size = format_settings.pretty.max_value_width * 4;
                if (serialized_value.size() > max_byte_size)
                    serialized_value.resize(max_byte_size);
            }

            widths[i][displayed_row] = UTF8::computeWidth(reinterpret_cast<const UInt8 *>(serialized_value.data()), serialized_value.size(), prefix);
            max_padded_widths[i] = std::max<UInt64>(
                max_padded_widths[i],
                std::min<UInt64>({format_settings.pretty.max_column_pad_width, format_settings.pretty.max_value_width, widths[i][displayed_row]}));

            ++displayed_row;
        }

        /// Also, calculate the widths for the names of columns.
        {
            auto [name, width] = truncateName(elem.name,
                format_settings.pretty.max_column_name_width_cut_to
                    ? std::max<UInt64>(max_padded_widths[i], format_settings.pretty.max_column_name_width_cut_to)
                    : 0,
                format_settings.pretty.max_column_name_width_min_chars_to_cut,
                format_settings.pretty.charset != FormatSettings::Pretty::Charset::UTF8);

            names[i] = std::move(name);
            name_widths[i] = std::min<UInt64>(format_settings.pretty.max_column_pad_width, width);
            max_padded_widths[i] = std::max<UInt64>(max_padded_widths[i], name_widths[i]);
        }
        prefix += max_padded_widths[i] + 3;
    }
}

void PrettyBlockOutputFormat::write(Chunk chunk, PortKind port_kind)
{
    if (total_rows >= format_settings.pretty.max_rows)
    {
        total_rows += chunk.getNumRows();
        return;
    }
    if (mono_block)
    {
        if (port_kind == PortKind::Main)
        {
            if (mono_chunk)
                mono_chunk.append(chunk);
            else
                mono_chunk = std::move(chunk);
            return;
        }

        /// Should be written from writeSuffix()
        assert(!mono_chunk);
    }

    writeChunk(chunk, port_kind);
}

void PrettyBlockOutputFormat::writeChunk(const Chunk & chunk, PortKind port_kind)
{
    auto num_rows = chunk.getNumRows();
    auto num_columns = chunk.getNumColumns();
    const auto & columns = chunk.getColumns();
    const auto & header = getPort(port_kind).getHeader();

    size_t cut_to_width = format_settings.pretty.max_value_width;
    if (!format_settings.pretty.max_value_width_apply_for_single_value && num_rows == 1 && num_columns == 1 && total_rows == 0)
        cut_to_width = 0;

    WidthsPerColumn widths;
    Widths max_widths;
    Widths name_widths;
    Strings names;
    calculateWidths(header, chunk, widths, max_widths, name_widths, names);

    /// Create separators

    String left_blank;
    if (format_settings.pretty.output_format_pretty_row_numbers)
        left_blank.assign(row_number_width, ' ');

    String header_begin;    /// ┏━━┳━━━┓
    String header_end;      /// ┡━━╇━━━┩
    String rows_separator;  /// ├──┼───┤
    String rows_end;        /// └──┴───┘
    String footer_begin;    /// ┢━━╈━━━┪
    String footer_end;      /// ┗━━┻━━━┛

    bool unicode = format_settings.pretty.charset == FormatSettings::Pretty::Charset::UTF8;
    using GridPart = std::array<std::string_view, 4>;
    using Grid = std::array<GridPart, 7>;

    constexpr Grid utf8_grid
    {
        GridPart{"┏", "━", "┳", "┓"},
        GridPart{"┡", "━", "╇", "┩"},
        GridPart{"├", "─", "┼", "┤"},
        GridPart{"└", "─", "┴", "┘"},
        GridPart{"┢", "━", "╈", "┪"},
        GridPart{"┗", "━", "┻", "┛"},
        GridPart{"┌", "─", "┬", "┐"},
    };

    constexpr Grid ascii_grid
    {
        GridPart{"+", "-", "+", "+"},
        GridPart{"+", "-", "+", "+"},
        GridPart{"+", "-", "+", "+"},
        GridPart{"+", "-", "+", "+"},
        GridPart{"+", "-", "+", "+"},
        GridPart{"+", "-", "+", "+"},
        GridPart{"+", "-", "+", "+"},
    };

    Grid grid = unicode ? utf8_grid : ascii_grid;

    std::string_view vertical_bold_bar   = unicode ? "┃" : "|";
    std::string_view vertical_bar        = unicode ? "│" : "|";
    std::string_view horizontal_bold_bar = unicode ? "━" : "-";
    std::string_view horizontal_bar      = unicode ? "─" : "-";

    if (style == Style::Full)
    {
        header_begin = left_blank;
        header_end = left_blank;
        rows_separator = left_blank;
        rows_end = left_blank;
        footer_begin = left_blank;
        footer_end = left_blank;

        WriteBufferFromString header_begin_out(header_begin, AppendModeTag{});
        WriteBufferFromString header_end_out(header_end, AppendModeTag{});
        WriteBufferFromString rows_separator_out(rows_separator, AppendModeTag{});
        WriteBufferFromString rows_end_out(rows_end, AppendModeTag{});
        WriteBufferFromString footer_begin_out(footer_begin, AppendModeTag{});
        WriteBufferFromString footer_end_out(footer_end, AppendModeTag{});

        header_begin_out    << grid[0][0];
        header_end_out      << grid[1][0];
        rows_separator_out  << grid[2][0];
        rows_end_out        << grid[3][0];
        footer_begin_out    << grid[4][0];
        footer_end_out      << grid[5][0];

        for (size_t i = 0; i < num_columns; ++i)
        {
            if (i != 0)
            {
                header_begin_out    << grid[0][2];
                header_end_out      << grid[1][2];
                rows_separator_out  << grid[2][2];
                rows_end_out        << grid[3][2];
                footer_begin_out    << grid[4][2];
                footer_end_out      << grid[5][2];
            }

            for (size_t j = 0; j < max_widths[i] + 2; ++j)
            {
                header_begin_out    << grid[0][1];
                header_end_out      << grid[1][1];
                rows_separator_out  << grid[2][1];
                rows_end_out        << grid[3][1];
                footer_begin_out    << grid[4][1];
                footer_end_out      << grid[5][1];
            }
        }

        header_begin_out    << grid[0][3] << "\n";
        header_end_out      << grid[1][3] << "\n";
        rows_separator_out  << grid[2][3] << "\n";
        rows_end_out        << grid[3][3] << "\n";
        footer_begin_out    << grid[4][3] << "\n";
        footer_end_out      << grid[5][3] << "\n";
    }
    else if (style == Style::Compact)
    {
        rows_end = left_blank;
        WriteBufferFromString rows_end_out(rows_end, AppendModeTag{});
        rows_end_out << grid[3][0];
        for (size_t i = 0; i < num_columns; ++i)
        {
            if (i != 0)
                rows_end_out        << grid[3][2];
            for (size_t j = 0; j < max_widths[i] + 2; ++j)
                rows_end_out        << grid[3][1];
        }
        rows_end_out        << grid[3][3] << "\n";
    }
    else if (style == Style::Space)
    {
        header_end = "\n";
        footer_begin = "\n";
        footer_end = "\n";
    }

    ///    ─ ─ ─ ─
    String vertical_filler = left_blank;

    {
        size_t vertical_filler_size = 0;
        WriteBufferFromString vertical_filler_out(vertical_filler, AppendModeTag{});

        for (size_t i = 0; i < num_columns; ++i)
            vertical_filler_size += max_widths[i] + 3;

        if (style == Style::Space)
            vertical_filler_size -= 2;
        else
            vertical_filler_size += 1;

        for (size_t i = 0; i < vertical_filler_size; ++i)
            vertical_filler_out << (i % 2 ? " " : horizontal_bar);

        vertical_filler_out << "\n";
    }

    ///    ┃ name ┃
    ///    ┌─name─┐
    ///    └─name─┘
    ///     name
    auto write_names = [&](bool is_top) -> void
    {
        writeString(left_blank, out);

        if (style == Style::Full)
            out << vertical_bold_bar << " ";
        else if (style == Style::Compact)
            out << grid[is_top ? 6 : 3][0] << horizontal_bar;
        else if (style == Style::Space)
            out << " ";

        for (size_t i = 0; i < num_columns; ++i)
        {
            if (i != 0)
            {
                if (style == Style::Full)
                    out << " " << vertical_bold_bar << " ";
                else if (style == Style::Compact)
                    out << horizontal_bar << grid[is_top ? 6 : 3][2] << horizontal_bar;
                else if (style == Style::Space)
                    out << "   ";
            }

            const auto & col = header.getByPosition(i);

            auto write_value = [&]
            {
                if (color)
                    out << "\033[1m";
                writeString(names[i], out);
                if (color)
                    out << "\033[0m";
            };

            auto write_padding = [&]
            {
                for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
                {
                    if (style == Style::Compact)
                        out << horizontal_bar;
                    else
                        out << " ";
                }
            };

            if (col.type->shouldAlignRightInPrettyFormats())
            {
                write_padding();
                write_value();
            }
            else
            {
                write_value();
                write_padding();
            }
        }
        if (style == Style::Full)
            out << " " << vertical_bold_bar;
        else if (style == Style::Compact)
            out << horizontal_bar << grid[is_top ? 6 : 3][3];

        out << "\n";
    };

    writeString(header_begin, out);
    write_names(true);
    writeString(header_end, out);

    bool vertical_filler_written = false;
    size_t displayed_row = 0;
    for (size_t i = 0; i < num_rows && displayed_rows < format_settings.pretty.max_rows; ++i)
    {
        if (cutInTheMiddle(i, num_rows, format_settings.pretty.max_rows))
        {
            if (!vertical_filler_written)
            {
                writeString(rows_separator, out);
                writeString(vertical_filler, out);
                vertical_filler_written = true;
            }
        }
        else
        {
            if (i != 0)
                writeString(rows_separator, out);

            if (format_settings.pretty.output_format_pretty_row_numbers)
            {
                /// Write row number;
                auto row_num_string = std::to_string(i + 1 + total_rows) + ". ";
                for (size_t j = 0; j < row_number_width - row_num_string.size(); ++j)
                    writeChar(' ', out);

                if (color)
                    out << "\033[90m";
                writeString(row_num_string, out);
                if (color)
                    out << "\033[0m";
            }

            for (size_t j = 0; j < num_columns; ++j)
            {
                if (style != Style::Space)
                    out << vertical_bar;
                else if (j != 0)
                    out << " ";

                const auto & type = *header.getByPosition(j).type;
                writeValueWithPadding(
                    *columns[j],
                    *serializations[j],
                    i,
                    widths[j].empty() ? max_widths[j] : widths[j][displayed_row],
                    max_widths[j],
                    cut_to_width,
                    type.shouldAlignRightInPrettyFormats(),
                    isNumber(type));
            }

            if (style != Style::Space)
                out << vertical_bar;

            if (readable_number_tip)
                writeReadableNumberTipIfSingleValue(out, chunk, format_settings, color);

            out << "\n";
            ++displayed_row;
            ++displayed_rows;
        }
    }

    /// output column names in the footer
    if ((num_rows >= format_settings.pretty.output_format_pretty_display_footer_column_names_min_rows) && format_settings.pretty.output_format_pretty_display_footer_column_names)
    {
        writeString(footer_begin, out);
        write_names(false);
        writeString(footer_end, out);
    }
    else
    {
        ///    └──────┘
        writeString(rows_end, out);
    }
    total_rows += num_rows;
}


void PrettyBlockOutputFormat::writeValueWithPadding(
    const IColumn & column, const ISerialization & serialization, size_t row_num,
    size_t value_width, size_t pad_to_width, size_t cut_to_width, bool align_right, bool is_number)
{
    String serialized_value;
    {
        WriteBufferFromString out_serialize(serialized_value, AppendModeTag());
        serialization.serializeText(column, row_num, out_serialize, format_settings);
    }

    /// Highlight groups of thousands.
    if (color && is_number && format_settings.pretty.highlight_digit_groups)
        serialized_value = highlightDigitGroups(serialized_value);

    /// Highlight trailing spaces.
    if (color && format_settings.pretty.highlight_trailing_spaces)
        serialized_value = highlightTrailingSpaces(serialized_value);

    bool is_cut = false;
    if (cut_to_width && value_width > cut_to_width)
    {
        is_cut = true;
        serialized_value.resize(UTF8::computeBytesBeforeWidth(
            reinterpret_cast<const UInt8 *>(serialized_value.data()), serialized_value.size(), 0, format_settings.pretty.max_value_width));

        const char * ellipsis = format_settings.pretty.charset == FormatSettings::Pretty::Charset::UTF8 ? "⋯" : "~";
        if (color)
        {
            serialized_value += "\033[31;1m";
            serialized_value += ellipsis;
            serialized_value += "\033[0m";
        }
        else
            serialized_value += ellipsis;

        value_width = format_settings.pretty.max_value_width;
    }

    auto write_padding = [&]()
    {
        if (pad_to_width > value_width)
            for (size_t k = 0; k < pad_to_width - value_width; ++k)
                writeChar(' ', out);
    };

    out.write(' ');

    if (align_right)
    {
        write_padding();
        out.write(serialized_value.data(), serialized_value.size());
    }
    else
    {
        out.write(serialized_value.data(), serialized_value.size());
        write_padding();
    }

    if (!is_cut)
        out.write(' ');
}


void PrettyBlockOutputFormat::consume(Chunk chunk)
{
    write(std::move(chunk), PortKind::Main);
}

void PrettyBlockOutputFormat::consumeTotals(Chunk chunk)
{
    total_rows = 0;
    writeCString("\nTotals:\n", out);
    write(std::move(chunk), PortKind::Totals);
}

void PrettyBlockOutputFormat::consumeExtremes(Chunk chunk)
{
    total_rows = 0;
    writeCString("\nExtremes:\n", out);
    write(std::move(chunk), PortKind::Extremes);
}


void PrettyBlockOutputFormat::writeMonoChunkIfNeeded()
{
    if (mono_chunk)
    {
        writeChunk(mono_chunk, PortKind::Main);
        mono_chunk.clear();
    }
}

void PrettyBlockOutputFormat::writeSuffix()
{
    writeMonoChunkIfNeeded();

    if (total_rows >= format_settings.pretty.max_rows)
    {
        if (style == Style::Space)
            out << "\n";

        out << "Showed " << displayed_rows << " out of " << total_rows << " rows.\n";
    }
}

void registerOutputFormatPretty(FormatFactory & factory)
{
    /// Various combinations are available under their own names, e.g. PrettyCompactNoEscapesMonoBlock.
    for (auto style : {PrettyBlockOutputFormat::Style::Full, PrettyBlockOutputFormat::Style::Compact, PrettyBlockOutputFormat::Style::Space})
    {
        for (bool no_escapes : {false, true})
        {
            for (bool mono_block : {false, true})
            {
                String name = "Pretty";

                if (style == PrettyBlockOutputFormat::Style::Compact)
                    name += "Compact";
                else if (style == PrettyBlockOutputFormat::Style::Space)
                    name += "Space";

                if (no_escapes)
                    name += "NoEscapes";
                if (mono_block)
                    name += "MonoBlock";

                factory.registerOutputFormat(name, [style, no_escapes, mono_block](
                    WriteBuffer & buf,
                    const Block & sample,
                    const FormatSettings & format_settings)
                {
                    bool color = !no_escapes
                        && (format_settings.pretty.color == 1 || (format_settings.pretty.color == 2 && format_settings.is_writing_to_terminal));
                    return std::make_shared<PrettyBlockOutputFormat>(buf, sample, format_settings, style, mono_block, color);
                });
            }
        }
    }
}

}
