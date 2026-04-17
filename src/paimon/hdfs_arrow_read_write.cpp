/*
 * Copyright 2024-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iostream>
#include <string>

#include "arrow/api.h"
#include "arrow/filesystem/hdfs.h"
#include "arrow/io/api.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

// 连接到 HDFS
arrow::Result<std::shared_ptr<arrow::fs::HadoopFileSystem>> ConnectHDFS(
    const std::string& host, int port, const std::string& user) {
    arrow::fs::HdfsOptions options;
    options.ConfigureEndPoint(host, port);
    options.ConfigureUser(user);

    auto fs_result = arrow::fs::HadoopFileSystem::Make(options);
    if (!fs_result.ok()) {
        return fs_result.status();
    }

    return fs_result.ValueOrDie();
}

// 写入 Parquet 文件到 HDFS
arrow::Status WriteParquetToHDFS(
    std::shared_ptr<arrow::fs::HadoopFileSystem> fs,
    const std::string& hdfs_path,
    std::shared_ptr<arrow::Table> table) {
    // 创建输出流
    auto output_stream_result = fs->OpenOutputStream(hdfs_path);
    if (!output_stream_result.ok()) {
        return output_stream_result.status();
    }
    auto output_stream = output_stream_result.ValueOrDie();

    // 写入 Parquet 文件
    auto write_result = parquet::arrow::WriteTable(
        *table,
        arrow::default_memory_pool(),
        output_stream,
        table->num_rows());
    if (!write_result.ok()) {
        return write_result;
    }

    // 关闭流
    auto close_result = output_stream->Close();
    if (!close_result.ok()) {
        return close_result;
    }

    std::cout << "成功写入文件: " << hdfs_path << std::endl;
    return arrow::Status::OK();
}

// 从 HDFS 读取 Parquet 文件
arrow::Status ReadParquetFromHDFS(
    std::shared_ptr<arrow::fs::HadoopFileSystem> fs,
    const std::string& hdfs_path) {
    // 打开输入流
    auto input_stream_result = fs->OpenInputFile(hdfs_path);
    if (!input_stream_result.ok()) {
        return input_stream_result.status();
    }
    auto input_stream = input_stream_result.ValueOrDie();

    // 创建 Parquet 读取器
    auto reader_result = parquet::arrow::OpenFile(input_stream, arrow::default_memory_pool());
    if (!reader_result.ok()) {
        return reader_result.status();
    }
    std::unique_ptr<parquet::arrow::FileReader> reader = reader_result.MoveValueUnsafe();

    // 读取数据
    std::shared_ptr<arrow::Table> table;
    auto status = reader->ReadTable(&table);
    if (!status.ok()) {
        return status;
    }

    // 打印结果
    std::cout << "\n=== 读取成功 ===" << std::endl;
    std::cout << "文件路径: " << hdfs_path << std::endl;
    std::cout << "行数: " << table->num_rows() << std::endl;
    std::cout << "列数: " << table->num_columns() << std::endl;
    std::cout << "\nSchema:" << std::endl;
    std::cout << table->schema()->ToString() << std::endl;
    std::cout << "\n数据内容:" << std::endl;
    std::cout << table->ToString() << std::endl;

    return arrow::Status::OK();
}

// 列出 HDFS 目录下的文件
arrow::Status ListHDFSFiles(
    std::shared_ptr<arrow::fs::HadoopFileSystem> fs,
    const std::string& hdfs_dir) {
    auto selector = arrow::fs::FileSelector();
    selector.base_dir = hdfs_dir;
    selector.recursive = false;

    auto file_info_result = fs->GetFileInfo(selector);
    if (!file_info_result.ok()) {
        return file_info_result.status();
    }

    std::cout << "\n=== HDFS 目录: " << hdfs_dir << " ===" << std::endl;
    for (const auto& info : file_info_result.ValueOrDie()) {
        std::cout << info.path() << " (" << info.type() << ")" << std::endl;
    }

    return arrow::Status::OK();
}

// 创建示例数据
arrow::Result<std::shared_ptr<arrow::Table>> CreateSampleTable() {
    arrow::StringBuilder name_builder;
    arrow::Int32Builder age_builder;
    arrow::DoubleBuilder score_builder;

    // 添加数据
    std::vector<std::string> names = {"Alice", "Bob", "Charlie", "David", "Eve"};
    std::vector<int32_t> ages = {25, 30, 35, 28, 32};
    std::vector<double> scores = {95.5, 87.3, 92.1, 88.9, 94.2};

    ARROW_RETURN_NOT_OK(name_builder.AppendValues(names));
    ARROW_RETURN_NOT_OK(age_builder.AppendValues(ages));
    ARROW_RETURN_NOT_OK(score_builder.AppendValues(scores));

    std::shared_ptr<arrow::Array> name_array, age_array, score_array;
    ARROW_RETURN_NOT_OK(name_builder.Finish(&name_array));
    ARROW_RETURN_NOT_OK(age_builder.Finish(&age_array));
    ARROW_RETURN_NOT_OK(score_builder.Finish(&score_array));

    std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("name", arrow::utf8()),
        arrow::field("age", arrow::int32()),
        arrow::field("score", arrow::float64())
    };

    auto schema = arrow::schema(fields);
    return arrow::Table::Make(schema, {name_array, age_array, score_array});
}

int main(int argc, char** argv) {
    // 默认参数
    std::string host = "localhost";
    int port = 9000;
    std::string user = "root";
    std::string hdfs_file_path = "/tmp/test_data2.parquet";
    std::string hdfs_dir_path = "/tmp";

    // 解析命令行参数
    if (argc >= 2) host = argv[1];
    if (argc >= 3) port = std::stoi(argv[2]);
    if (argc >= 4) user = argv[3];
    if (argc >= 5) hdfs_file_path = argv[4];

    std::cout << "HDFS 读写示例" << std::endl;
    std::cout << "连接参数: " << host << ":" << port << " (user: " << user << ")" << std::endl;

    // 连接 HDFS
    std::cout << "\n正在连接 HDFS..." << std::endl;
    auto fs_result = ConnectHDFS(host, port, user);
    if (!fs_result.ok()) {
        std::cerr << "连接 HDFS 失败：" << fs_result.status().ToString() << std::endl;
        return -1;
    }
    auto fs = fs_result.ValueOrDie();
    std::cout << "成功连接到 HDFS" << std::endl;

    // 列出目录
    auto list_status = ListHDFSFiles(fs, hdfs_dir_path);
    if (!list_status.ok()) {
        std::cerr << "列出目录失败：" << list_status.ToString() << std::endl;
    }

    // 创建示例数据
    std::cout << "\n正在创建示例数据..." << std::endl;
    auto table_result = CreateSampleTable();
    if (!table_result.ok()) {
        std::cerr << "创建数据失败：" << table_result.status().ToString() << std::endl;
        return -1;
    }
    auto table = table_result.ValueOrDie();
    std::cout << "示例数据创建完成" << std::endl;
    std::cout << table->ToString() << std::endl;

    // 写入 Parquet 文件
    std::cout << "\n正在写入 Parquet 文件到 HDFS..." << std::endl;
    auto write_status = WriteParquetToHDFS(fs, hdfs_file_path, table);
    if (!write_status.ok()) {
        std::cerr << "写入文件失败：" << write_status.ToString() << std::endl;
        return -1;
    }

    // 读取 Parquet 文件
    std::cout << "\n正在从 HDFS 读取 Parquet 文件..." << std::endl;
    auto read_status = ReadParquetFromHDFS(fs, hdfs_file_path);
    if (!read_status.ok()) {
        std::cerr << "读取文件失败：" << read_status.ToString() << std::endl;
        return -1;
    }

    std::cout << "\n=== 示例执行完成 ===" << std::endl;
    return 0;
}
