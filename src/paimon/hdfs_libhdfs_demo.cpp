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
#include <cstring>

#ifdef __cplusplus
extern "C" {
#endif

#include "hdfs.h"

#ifdef __cplusplus
}
#endif

/**
 * 连接到 HDFS
 */
hdfsFS ConnectHDFS(const std::string& namenode, int port, const std::string& user) {
    struct hdfsBuilder* builder = hdfsNewBuilder();
    if (!builder) {
        std::cerr << "创建 HDFS builder 失败" << std::endl;
        return nullptr;
    }

    hdfsBuilderSetNameNode(builder, namenode.c_str());
    hdfsBuilderSetNameNodePort(builder, port);
    hdfsBuilderSetUserName(builder, user.c_str());

    hdfsFS fs = hdfsBuilderConnect(builder);
    if (!fs) {
        std::cerr << "连接 HDFS 失败: " << namenode << ":" << port << std::endl;
        return nullptr;
    }

    std::cout << "成功连接到 HDFS: " << namenode << ":" << port << std::endl;
    return fs;
}

/**
 * 断开 HDFS 连接
 */
void DisconnectHDFS(hdfsFS fs) {
    if (fs) {
        hdfsDisconnect(fs);
        std::cout << "已断开 HDFS 连接" << std::endl;
    }
}

/**
 * 写入文件到 HDFS
 */
bool WriteFileToHDFS(hdfsFS fs, const std::string& hdfs_path, const std::string& content) {
    // 检查文件是否存在，如果存在则删除
    if (hdfsExists(fs, hdfs_path.c_str()) == 0) {
        std::cout << "文件已存在，删除旧文件: " << hdfs_path << std::endl;
        if (hdfsDelete(fs, hdfs_path.c_str(), 0) != 0) {
            std::cerr << "删除文件失败" << std::endl;
            return false;
        }
    }

    // 打开文件进行写入
    hdfsFile file = hdfsOpenFile(fs, hdfs_path.c_str(), O_WRONLY, 0, 0, 0);
    if (!file) {
        std::cerr << "打开文件失败: " << hdfs_path << std::endl;
        return false;
    }

    // 写入内容
    tSize bytes_written = hdfsWrite(fs, file, content.c_str(), content.length());
    if (bytes_written != content.length()) {
        std::cerr << "写入文件失败: 期望 " << content.length() << " 字节, 实际 " << bytes_written << " 字节" << std::endl;
        hdfsCloseFile(fs, file);
        return false;
    }

    // 关闭文件
    if (hdfsCloseFile(fs, file) != 0) {
        std::cerr << "关闭文件失败" << std::endl;
        return false;
    }

    std::cout << "成功写入文件: " << hdfs_path << " (" << bytes_written << " 字节)" << std::endl;
    return true;
}

/**
 * 从 HDFS 读取文件
 */
bool ReadFileFromHDFS(hdfsFS fs, const std::string& hdfs_path, std::string& content) {
    // 获取文件信息
    hdfsFileInfo* file_info = hdfsGetPathInfo(fs, hdfs_path.c_str());
    if (!file_info) {
        std::cerr << "获取文件信息失败: " << hdfs_path << std::endl;
        return false;
    }

    tSize file_size = file_info->mSize;
    std::cout << "文件大小: " << file_size << " 字节" << std::endl;
    hdfsFreeFileInfo(file_info, 1);

    // 打开文件进行读取
    hdfsFile file = hdfsOpenFile(fs, hdfs_path.c_str(), O_RDONLY, 0, 0, 0);
    if (!file) {
        std::cerr << "打开文件失败: " << hdfs_path << std::endl;
        return false;
    }

    // 读取内容
    char* buffer = new char[file_size + 1];
    tSize bytes_read = hdfsRead(fs, file, buffer, file_size);
    buffer[bytes_read] = '\0';

    // 关闭文件
    hdfsCloseFile(fs, file);

    if (bytes_read < 0) {
        std::cerr << "读取文件失败" << std::endl;
        delete[] buffer;
        return false;
    }

    content = buffer;
    delete[] buffer;

    std::cout << "成功读取文件: " << hdfs_path << " (" << bytes_read << " 字节)" << std::endl;
    return true;
}

/**
 * 列出 HDFS 目录内容
 */
bool ListHDFSDirectory(hdfsFS fs, const std::string& hdfs_dir) {
    int num_entries = 0;
    hdfsFileInfo* file_info = hdfsListDirectory(fs, hdfs_dir.c_str(), &num_entries);
    
    if (!file_info) {
        std::cerr << "列出目录失败: " << hdfs_dir << std::endl;
        return false;
    }

    std::cout << "\n=== 目录内容: " << hdfs_dir << " ===" << std::endl;
    std::cout << "文件/目录数量: " << num_entries << std::endl;

    std::cout << std::string(60, '-') << std::endl;

    for (int i = 0; i < num_entries; i++) {
        std::string type = (file_info[i].mKind == kObjectKindDirectory) ? "目录" : "文件";

    }

    hdfsFreeFileInfo(file_info, num_entries);
    return true;
}

/**
 * 创建 HDFS 目录
 */
bool CreateHDFSDirectory(hdfsFS fs, const std::string& hdfs_path) {
    if (hdfsExists(fs, hdfs_path.c_str()) == 0) {
        std::cout << "目录已存在: " << hdfs_path << std::endl;
        return true;
    }

    if (hdfsCreateDirectory(fs, hdfs_path.c_str()) != 0) {
        std::cerr << "创建目录失败: " << hdfs_path << std::endl;
        return false;
    }

    std::cout << "成功创建目录: " << hdfs_path << std::endl;
    return true;
}

/**
 * 获取 HDFS 文件系统统计信息
 */
void PrintHDFSStatistics(hdfsFS fs) {
    tOffset capacity = hdfsGetCapacity(fs);
    tOffset used = hdfsGetUsed(fs);
    
    std::cout << "\n=== HDFS 统计信息 ===" << std::endl;
    std::cout << "总容量: " << capacity << " 字节 (" 
              << (capacity / 1024.0 / 1024.0 / 1024.0) << " GB)" << std::endl;
    std::cout << "已使用: " << used << " 字节 (" 
              << (used / 1024.0 / 1024.0 / 1024.0) << " GB)" << std::endl;
    std::cout << "可用空间: " << (capacity - used) << " 字节 (" 
              << ((capacity - used) / 1024.0 / 1024.0 / 1024.0) << " GB)" << std::endl;
}

/**
 * 删除 HDFS 文件或目录
 */
bool DeleteHDFSPath(hdfsFS fs, const std::string& hdfs_path, bool recursive) {
    if (hdfsExists(fs, hdfs_path.c_str()) != 0) {
        std::cout << "路径不存在: " << hdfs_path << std::endl;
        return true;
    }

    if (hdfsDelete(fs, hdfs_path.c_str(), recursive ? 1 : 0) != 0) {
        std::cerr << "删除路径失败: " << hdfs_path << std::endl;
        return false;
    }

    std::cout << "成功删除路径: " << hdfs_path << std::endl;
    return true;
}

int main(int argc, char** argv) {
    // 默认参数
    std::string namenode = "localhost";
    int port = 9000;
    std::string user = "root";
    std::string hdfs_dir_path = "/warehouse";
    std::string hdfs_file_path = "/warehouse/mydb.db/mytable/bucket-0/data-15db4615-ee7a-466b-b8aa-d012dd4c3594-0.parquet";

    // 解析命令行参数
    if (argc >= 2) namenode = argv[1];
    if (argc >= 3) port = std::stoi(argv[2]);
    if (argc >= 4) user = argv[3];
    if (argc >= 5) hdfs_file_path = argv[4];
    if (argc >= 6) hdfs_dir_path = argv[5];

    std::cout << "=== libhdfs HDFS 读写示例 ===" << std::endl;
    std::cout << "连接参数: " << namenode << ":" << port << " (user: " << user << ")" << std::endl;

    // 连接 HDFS
    std::cout << "\n正在连接 HDFS..." << std::endl;
    hdfsFS fs = ConnectHDFS(namenode, port, user);
    if (!fs) {
        std::cerr << "无法连接到 HDFS" << std::endl;
        return -1;
    }

    // 打印统计信息
    PrintHDFSStatistics(fs);

    // 创建目录
    std::cout << "\n正在创建测试目录..." << std::endl;
    if (!CreateHDFSDirectory(fs, hdfs_dir_path)) {
        std::cerr << "创建目录失败" << std::endl;
    }

    // 写入文件
    std::cout << "\n正在写入文件到 HDFS..." << std::endl;
    std::string content = "Hello from libhdfs!\n"
                         "这是一个使用 libhdfs API 的 C++ 示例。\n"
                         "我们可以通过 C/C++ 代码直接操作 HDFS 文件系统。\n"
                         "这包括创建目录、读写文件、列出文件列表等操作。\n";
    
    if (!WriteFileToHDFS(fs, hdfs_file_path, content)) {
        std::cerr << "写入文件失败" << std::endl;
        DisconnectHDFS(fs);
        return -1;
    }

    // 列出目录
    std::cout << "\n正在列出 /tmp 目录内容..." << std::endl;
    ListHDFSDirectory(fs, "/warehouse");

    // 读取文件
    std::cout << "\n正在从 HDFS 读取文件..." << std::endl;
    std::string read_content;
    if (!ReadFileFromHDFS(fs, hdfs_file_path, read_content)) {
        std::cerr << "读取文件失败" << std::endl;
        DisconnectHDFS(fs);
        return -1;
    }

    // 显示读取的内容
    std::cout << "\n=== 文件内容 ===" << std::endl;
    std::cout << read_content << std::endl;

    // 追加写入示例
    std::cout << "\n正在追加内容到文件..." << std::endl;
    hdfsFile file = hdfsOpenFile(fs, hdfs_file_path.c_str(), O_WRONLY | O_APPEND, 0, 0, 0);
    if (file) {
        std::string append_content = "\n--- 这是追加的内容 ---\n";
        hdfsWrite(fs, file, append_content.c_str(), append_content.length());
        hdfsCloseFile(fs, file);
        std::cout << "成功追加内容" << std::endl;
    }

    // 再次读取验证追加
    std::cout << "\n再次读取文件验证追加..." << std::endl;
    ReadFileFromHDFS(fs, hdfs_file_path, read_content);
    std::cout << read_content << std::endl;

    // 断开连接
    std::cout << "\n正在断开 HDFS 连接..." << std::endl;
    DisconnectHDFS(fs);

    std::cout << "\n=== 示例执行完成 ===" << std::endl;
    std::cout << "\n提示: 可以使用以下命令清理测试文件" << std::endl;
    std::cout << "  hdfs dfs -rm " << hdfs_file_path << std::endl;
    std::cout << "  hdfs dfs -rmdir " << hdfs_dir_path << std::endl;

    return 0;
}
