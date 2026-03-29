#include <iostream>
#include <string>

#include "arrow/api.h"
#include "arrow/filesystem/hdfs.h"
#include "parquet/arrow/reader.h"

int main() {
    std::string hdfs_path = "/warehouse/mydb.db/mytable/bucket-0/data-15db4615-ee7a-466b-b8aa-d012dd4c3594-0.parquet";

    std::cout << "HADOOP_HOME: " << (std::getenv("HADOOP_HOME") ? std::getenv("HADOOP_HOME") : "NULL") << std::endl;
    std::cout << "LD_LIBRARY_PATH: " << (std::getenv("LD_LIBRARY_PATH") ? std::getenv("LD_LIBRARY_PATH") : "NULL") << std::endl;
    std::cout << "CLASSPATH: " << (std::getenv("CLASSPATH") ? std::getenv("CLASSPATH") : "NULL") << std::endl;


    std::cout << "正在连接 HDFS..." << std::endl;

    arrow::fs::HdfsOptions options;
    options.ConfigureEndPoint("localhost", 9000);
    options.ConfigureUser("root");

    auto fs_result = arrow::fs::HadoopFileSystem::Make(options);
    if (!fs_result.ok()) {
        std::cerr << "连接 HDFS 失败：" << fs_result.status().ToString() << std::endl;
        return -1;
    }
    
    auto fs = fs_result.ValueOrDie();
    std::cout << "成功连接到 HDFS" << std::endl;
    
    std::cout << "正在读取文件：" << hdfs_path << std::endl;
    
    auto input_stream_result = fs->OpenInputFile(hdfs_path);
    if (!input_stream_result.ok()) {
        std::cerr << "打开文件失败：" << input_stream_result.status().ToString() << std::endl;
        return -1;
    }
    
    auto input_stream = input_stream_result.ValueOrDie();
    
    auto reader_result = parquet::arrow::OpenFile(input_stream, arrow::default_memory_pool());
    if (!reader_result.ok()) {
        std::cerr << "创建 Parquet 读取器失败：" << reader_result.status().ToString() << std::endl;
        return -1;
    }
    
    std::unique_ptr<parquet::arrow::FileReader> reader = reader_result.MoveValueUnsafe();

    std::shared_ptr<arrow::Table> table;
    auto status = reader->ReadTable(&table);
    if (!status.ok()) {
        std::cerr << "读取数据失败：" << status.ToString() << std::endl;
        return -1;
    }
    
    std::cout << "=== 读取成功 ===" << std::endl;
    std::cout << "行数：" << table->num_rows() << std::endl;
    std::cout << "列数：" << table->num_rows() << std::endl;
    std::cout << "\nSchema:" << std::endl;
    std::cout << table->schema()->ToString() << std::endl;
    std::cout << "\n数据内容:" << std::endl;
    std::cout << table->ToString() << std::endl;
    
    return 0;
}
