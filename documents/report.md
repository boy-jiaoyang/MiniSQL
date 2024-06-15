<div class="cover" style="page-break-after:always;font-family:方正公文仿宋;width:100%;height:100%;border:none;margin: 0 auto;text-align:center;">
    <div style="width:60%;margin: 0 auto;height:0;padding-bottom:10%;">
        </br>
        <img src="https://raw.githubusercontent.com/Keldos-Li/pictures/main/typora-latex-theme/ZJU-name.svg" alt="校名" style="width:100%;"/>
    </div>
    </br></br></br></br></br>
    <div style="width:60%;margin: 0 auto;height:0;padding-bottom:40%;">
        <img src="https://raw.githubusercontent.com/Keldos-Li/pictures/main/typora-latex-theme/ZJU-logo.svg" alt="校徽" style="width:100%;"/>
	</div>
    </br></br></br></br></br></br></br></br>
    <span style="font-family:华文黑体Bold;text-align:center;font-size:20pt;margin: 10pt auto;line-height:30pt;">数据库系统: PROJECT</span>
    <p style="text-align:center;font-size:14pt;margin: 0 auto"> </p>
    </br>
    </br>
    <table style="border:none;text-align:center;width:72%;font-family:仿宋;font-size:14px; margin: 0 auto;">
    <tbody style="font-family:方正公文仿宋;font-size:12pt;">
    	<tr style="font-weight:normal;"> 
    		<td style="width:20%;text-align:right;">题　　目</td>
    		<td style="width:2%">：</td> 
    		<td style="width:40%;font-weight:normal;border-bottom: 1px solid;text-align:center;font-family:华文仿宋"> MiniSQL</td>     </tr>
    	<tr style="font-weight:normal;"> 
    		<td style="width:20%;text-align:right;">授课教师</td>
    		<td style="width:2%">：</td> 
    		<td style="width:40%;font-weight:normal;border-bottom: 1px solid;text-align:center;font-family:华文仿宋">周波 </td>     </tr>
    	<tr style="font-weight:normal;"> 
    		<td style="width:20%;text-align:right;">姓　　名</td>
    		<td style="width:2%">：</td> 
    		<td style="width:40%;font-weight:normal;border-bottom: 1px solid;text-align:center;font-family:华文仿宋"> 陈奕萱 焦洋</td>     </tr>
    	<tr style="font-weight:normal;"> 
    		<td style="width:20%;text-align:right;">学　　号</td>
    		<td style="width:2%">：</td> 
    		<td style="width:40%;font-weight:normal;border-bottom: 1px solid;text-align:center;font-family:华文仿宋">3220102866 3220102087 </td>     </tr>
    	<tr style="font-weight:normal;"> 
    		<td style="width:20%;text-align:right;">日　　期</td>
    		<td style="width:2%">：</td> 
    		<td style="width:40%;font-weight:normal;border-bottom: 1px solid;text-align:center;font-family:华文仿宋">2024-06-10</td>     </tr>
    </tbody>              
    </table>
</div>



<center><div style='height:15mm;'></div><div style="font-family:Times New Roman;font-size:20pt;"> 目录</div></center><br><br>

[toc]

<div STYLE="page-break-after: always;"></div>

## 1 实验目的

1. 设计并实现一个精简型单用户SQL引擎MiniSQL，允许用户通过字符界面输入SQL语句实现基本的增删改查操作，并能够通过索引来优化性能。
2. 通过对MiniSQL的设计与实现，提高学生的系统编程能力，加深对数据库管理系统底层设计的理解。

## 2 实验需求

1. 数据类型：要求支持三种基本数据类型：`integer`，`char(n)`，`float`。
2. 表定义：一个表可以定义多达32个属性，各属性可以指定是否为`unique`，支持单属性的主键定义。
3. 索引定义：对于表的主属性自动建立B+树索引，对于声明为`unique`的属性也需要建立B+树索引。
4. 数据操作: 可以通过`and`或`or`连接的多个条件进行查询，支持等值查询和区间查询。支持每次一条记录的插入操作；支持每次一条或多条记录的删除操作。
5. 在工程实现上，使用源代码管理工具（如Git）进行代码管理，代码提交历史和每次提交的信息清晰明确；同时编写的代码应符合代码规范，具有良好的代码风格。

## 3 实验平台

### 3.1 代码框架

本实验基于CMU-15445 BusTub框架，课程组做了一些修改和扩展。

### 3.2 编译&开发环境

>  使用WSL-Ubuntu + CLion进行开发

- `gcc`&`g++` : 8.0+ (Linux)，使用`gcc --version`和`g++ --version`查看

![](assets/1.png)

- `cmake`: 3.16+ (Both)，使用`cmake --version`查看

![alt text](assets/image1.png)

- `gdb`: 7.0+ (Optional)，使用`gdb --version`查看

![alt text](assets/image2.png) 

- `flex`& `bison`（暂时不需要安装，但如果需要对SQL编译器的语法进行修改，需要安装）

CLion配置：

![alt text](assets/image3.png)

## 4 实验模块

### 4.1 DISK AND BUFFER POOL MANAGER

#### 4.1.1 概述

第一个模块包含了四个部分，分别是

- 位图页实现
- 磁盘数据页管理
- 缓冲池替换策略
    - LRU替换策略
    - **(Bonus)** CLOCK替换策略
- 缓冲池管理


**它这个底下是这么分的，我不太清楚，要是你不好写应该也可以不这么分吧**

#### 4.1.2 Bitmap 实现

​	位图页是Disk Manager模块中的一部分，是实现磁盘页分配与回收工作的必要功能组件。位图页与数据页一样，占用`PAGE_SIZE`（4KB）的空间，标记一段连续页的分配情况。

​    Bitmap Page由两部分组成，一部分是用于加速Bitmap内部查找的元信息（Bitmap Page Meta），它包含当前已经分配的页的数量（`page_allocated_`）以及下一个空闲的数据页(`next_free_page_`)。除去元信息外，页中剩余的部分就是Bitmap存储的具体数据，其大小`BITMAP_CONTENT_SIZE`可以通过`PAGE_SIZE - BITMAP_PAGE_META_SIZE`来计算，自然而然，这个Bitmap Page能够支持最多纪录`BITMAP_CONTENT_SIZE * 8`个连续页的分配情况。

​	下面是我实现的函数的思路

1. `bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset)`

- 检查是否所有页面都已经分配完毕，如果是，返回 `false`。
- 设置 `page_offset` 为下一个空闲页面 `next_free_page_`。
- 计算该页在字节数组 `bytes` 中的字节索引 `byte_index` 和位索引 `bit_index`。
- 使用 `XOR` 操作将该位从0翻转为1，表示该页已分配。
- 更新 `next_free_page_` 为下一个空闲页索引。首先从当前分配页之后寻找，若未找到，则从当前页之前寻找。
- 增加已分配页计数 `page_allocated_`。
- 返回 `true` 表示成功分配页面。

2. `bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset)`

- 检查要释放的页是否已经是空闲状态，如果是，返回 `false`。
- 计算该页在字节数组 `bytes` 中的字节索引 `byte_index` 和位索引 `bit_index`。
- 使用 `XOR` 操作将该位从1翻转为0，表示该页已释放。
- 更新 `next_free_page_` 为刚释放的页索引。
- 减少已分配页计数 `page_allocated_`。
- 返回 `true` 表示成功释放页面。

3. `bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const`

- 通过调用 `IsPageFreeLow` 函数检查指定页是否为空闲。
- 将页偏移 `page_offset` 转换为字节索引和位索引传递给 `IsPageFreeLow`。

4. `bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const`

- 检查字节索引是否超过最大字节数 `MAX_CHARS`，若超过返回 `false`。
- 检查指定字节和位上的值是否为0（表示空闲）。具体通过将该字节右移 `bit_index` 位，然后与1进行与运算，最后对结果进行异或操作（0变1，1变0），若为0表示该位为空闲。

#### 4.1.3 磁盘数据页管理

​	在实现了基本的位图页后，我们就可以通过一个位图页加上一段连续的数据页（数据页的数量取决于位图页最大能够支持的比特数）来对磁盘文件（DB File）中数据页进行分配和回收。但实际上，这样的设计还存在着一点点的小问题，假设数据页的大小为4KB，一个位图页中的每个字节都用于记录，那么这个位图页最多能够管理32768个数据页，也就是说，这个文件最多只能存储`4K * 8 * 4KB = 128MB`的数据，这实际上很容易发生数据溢出的情况。

​	为了应对上述问题，一个简单的解决思路是，把上面说的一个位图页加一段连续的数据页看成数据库文件中的一个分区（Extent），再通过一个额外的元信息页来记录这些分区的信息。

​	下面是我实现的函数的思路：

1. `page_id_t DiskManager::AllocatePage()`

- 从元数据页面 `DiskFileMetaPage` 获取元数据，包括现有的扩展数量 `num_extents_`。
- 遍历每个扩展，查找一个未满的扩展（即 `extent_used_page_` 小于 `BITMAP_SIZE`）。
- 计算对应的物理页框 ID `allocated_frame_id`。
- 如果所有现有扩展都已满，则创建一个新的扩展并写入磁盘，并更新元数据页面中的扩展数量和使用情况。
- 读取已找到或新创建的扩展的物理页数据。
- 调用 `AllocatePage` 分配页面，更新元数据页面的分配页面数量和扩展的使用情况。
- 将更新后的扩展写回磁盘。
- 返回分配的逻辑页面 ID。

2. `void DiskManager::DeAllocatePage(page_id_t logical_page_id)`

- 确认逻辑页面 ID 在有效范围内。
- 从元数据页面获取元数据。
- 计算要释放的页面所在的扩展 ID 和页框 ID。
- 读取该扩展的物理页数据。
- 调用 `DeAllocatePage` 释放页面。
- 更新元数据页面的分配页面数量和该扩展的使用情况。
- 将更新后的扩展写回磁盘。

3. `bool DiskManager::IsPageFree(page_id_t logical_page_id)`

- 确认逻辑页面 ID 在有效范围内，如果超出返回 `false`。
- 计算逻辑页面 ID 所在的扩展 ID 和页框 ID。
- 读取该扩展的物理页数据。
- 调用 `IsPageFree` 检查页面是否空闲。
- 返回结果。

4. `page_id_t DiskManager::MapPageId(page_id_t logical_page_id)`

- 计算逻辑页面 ID 所在的扩展页面编号。
- 返回逻辑页面 ID 和扩展页面编号的和(物理页面 ID )。

#### 4.1.4 LRU替换策略

​	LRU 替换策略是最近最少访问者被替换，我的实现方式是用一个链表记录 frame_id ，用哈希表记录 frame_id 在链表中的位置。效果类似于一个队列，每次 Unpin 时，从链表末尾加入，每次寻找 Victim 时，则从链表头取出。

​	下面是我实现的函数的思路：

1. `bool LRUReplacer::Victim(frame_id_t *frame_id)`

- 检查 LRU 列表 `lru_list_` 是否为空。如果为空，返回 `false` 表示没有页面可以被替换。
- 如果不为空，取出 `lru_list_` 的最后一个元素（这是最久未使用的页面），将其赋值给 `frame_id`。
- 从 `lru_list_` 中移除该元素，并从未固定集合 `lru_unpin_set_` 中删除该页面。
- 返回 `true` 表示成功选择了一个可替换页面。

2. `void LRUReplacer::Pin(frame_id_t frame_id)`

- 如果当前未固定页面的数量已经达到了最大页面数 `max_pages_`，直接返回（这种情况下不需要固定更多页面）。
- 检查 `frame_id` 是否在 `lru_unpin_set_` 中。如果在：
  - 从 `lru_unpin_set_` 中删除该 `frame_id`。
  - 从 `lru_list_` 中移除该 `frame_id`。

3. `void LRUReplacer::Unpin(frame_id_t frame_id)`

- 如果当前未固定页面的数量已经达到了最大页面数 `max_pages_`，直接返回（不需要增加更多页面）。
- 检查 `frame_id` 是否在 `lru_unpin_set_` 中。如果不在：
  - 将 `frame_id` 插入 `lru_unpin_set_`。
  - 将 `frame_id` 插入 `lru_list_` 的前面（表示最近使用过）。

4. `size_t LRUReplacer::Size()`

- 返回 `lru_list_` 的大小，即当前未固定的页面数。

#### 4.1.5 缓冲池管理

​	Buffer Pool Manager负责从Disk Manager中获取数据页并将它们存储在内存中，并在必要时将脏页面转储到磁盘中（如需要为新的页面腾出空间）。

​	下面是我实现的函数的思路：

1. `Page *BufferPoolManager::FetchPage(page_id_t page_id)`

- 检查 `page_id` 是否有效。如果无效，返回 `nullptr`。
- 如果页面表中存在 `page_id`，将其固定并返回对应的页面。
- 如果页面表中不存在 `page_id`：
  - 如果没有空闲页框，从替换器中找到一个受害页面，如果该页面是脏的，写回磁盘，并从页面表中删除。
  - 如果有空闲页框，从空闲列表中取出一个页框。
- 将新的页面插入页面表，从磁盘读取该页面的数据，更新页面元数据。
- 返回页面指针。

2. `Page *BufferPoolManager::NewPage(page_id_t &page_id)`

- 检查是否有空闲页框。如果有，从空闲列表中取出一个页框。
- 分配一个新的页面 ID。
- 更新页面元数据，将其插入页面表，返回页面指针。
- 如果没有空闲页框，从替换器中找到一个受害页面。
- 分配一个新的页面 ID。
- 如果受害页面是脏的，写回磁盘，并从页面表中删除。
- 更新页面元数据，将其插入页面表，返回页面指针。

3. `bool BufferPoolManager::DeletePage(page_id_t page_id)`

- 查找页面表中是否存在 `page_id`，如果不存在，返回 `true`。
- 如果页面存在，但固定计数不为 0，返回 `false`。
- 如果页面是脏的，刷新页面到磁盘。
- 从页面表中删除该页面，重置页面元数据，将页框返回到空闲列表。
- 释放页面。

4. `bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty)`

- 检查页面表中是否存在 `page_id`，如果不存在，返回 `false`。
- 更新页面的脏状态。
- 如果页面的固定计数大于 0，递减固定计数。
- 如果固定计数为 0，将页面加入替换器中。
- 返回 `true`。

5. `bool BufferPoolManager::FlushPage(page_id_t page_id)`

- 检查页面表中是否存在 `page_id`，如果存在：
  - 获取对应的页框 ID。
  - 将页面内容写回磁盘。
- 返回 `true`。

#### 4.1.6 测试结果

![alt text](assets/image4.png)

![alt text](assets/image.png)

![alt text](assets/image-1.png)

### 4.2 RECORD MANAGER

#### 4.2.1 概述

​	在MiniSQL的设计中，Record Manager负责管理数据表中所有的记录，它能够支持记录的插入、删除与查找操作，并对外提供相应的接口。

​	与记录（Record）相关的概念有以下几个：

- 列（`Column`）：在`src/include/record/column.h`中被定义，用于定义和表示数据表中的某一个字段，即包含了这个字段的字段名、字段类型、是否唯一等等；
- 模式（`Schema`）：在`src/include/record/schema.h`中被定义，用于表示一个数据表或是一个索引的结构。一个`Schema`由一个或多个的`Column`构成；
- 域（`Field`）：在`src/include/record/field.h`中被定义，它对应于一条记录中某一个字段的数据信息，如存储数据的数据类型，是否是空，存储数据的值等等；
- 行（`Row`）：在`src/include/record/row.h`中被定义，与元组的概念等价，用于存储记录或索引键，一个`Row`由一个或多个`Field`构成。

​	此外，与数据类型相关的定义和实现位于`src/include/record/types.h`中。

#### 4.2.2 记录与模式

​	这部分我主要实现的是序列化和反序列化。

​	为了持久化数据，我们需要对 Row，Column，Schema 和 Field 进行序列化处理，以便它能够存到磁盘中。此外引入魔数做为简单的检验数据正确性的手段。

​	序列化也即在上游像“水流”一样将数据按字节存到一块连续的内存区域（buffer）中，反序列化即在“下游”从 buffer 中按顺序取出存的东西再重新构造出相应的对象。

#### 4.2.3 Row

序列化：

1. **断言检查**：
  - 确保 `schema` 不为空。
  - 确保 `Row` 对象中的字段数量与 `schema` 中定义的列数量一致。

2. **初始化缓冲区指针**：
  - 使用指针 `p` 保存缓冲区的起始位置。

3. **计算位图大小**：
  - 位图用于表示哪些字段是 `NULL`。位图大小为 `(fields_.size() + 7) / 8`，以字节为单位。

4. **初始化位图**：
  - 分配位图内存并初始化为0。
  - 遍历所有字段，根据字段是否为 `NULL` 设置位图的相应位。

5. **序列化位图大小**：
  - 将位图大小写入缓冲区。

6. **序列化位图**：
  - 将位图内容写入缓冲区。

7. **序列化字段数据**：
  - 遍历 `fields_`，调用每个字段的 `SerializeTo` 方法将其数据序列化到缓冲区中。

8. **返回序列化后的数据长度**：
  - 通过 `buf - p` 计算序列化后的数据长度。

反序列化：

1. **断言检查**：
  - 确保 `schema` 不为空。
  - 确保 `fields_` 为空，即 `Row` 对象中没有预先存在的字段数据。

2. **初始化缓冲区指针**：
  - 使用指针 `p` 保存缓冲区的起始位置。

3. **读取位图大小**：
  - 从缓冲区读取位图的大小。

4. **读取位图内容**：
  - 从缓冲区读取位图内容。

5. **获取 `schema` 的列定义**：
  - 获取 `schema` 中定义的列信息。

6. **调整 `fields_` 大小**：
  - 将 `fields_` 的大小调整为与 `schema` 的列数一致。

7. **反序列化字段数据**：
  - 遍历所有列，根据位图判断字段是否为 `NULL`，调用 `Field::DeserializeFrom` 方法将字节流中的数据反序列化到相应的字段中。

8. **返回反序列化后的数据长度**：
  - 通过 `buf - p` 计算反序列化后的数据长度。

#### 4.2.4 Column

序列化：

1. **断言检查**：
  - 确保序列化后的数据大小不超过 `PAGE_SIZE`。

2. **初始化缓冲区指针**：
  - 使用指针 `p` 保存缓冲区的起始位置。

3. **写入魔数**：
  - 魔数（magic number）用于标识数据结构的类型，方便后续反序列化时进行校验。

4. **写入列名长度**：
  - 获取列名的长度并写入缓冲区。

5. **写入列名字符串**：
  - 将列名字符串写入缓冲区。

6. **写入其他字段**：
  - 依次写入列的数据类型（type）、长度（len_）、列索引（table_ind_）、可为空标志（nullable_）和唯一标志（unique_）。

7. **返回序列化后的数据长度**：
  - 通过 `buf - p` 计算序列化后的数据长度。

反序列化：

1. **断言检查**：
  - 确保 `column` 指针为空，以避免内存泄漏或重复初始化。

2. **初始化缓冲区指针**：
  - 使用指针 `p` 保存缓冲区的起始位置。

3. **分配内存**：
  - 为 `Column` 对象分配内存。

4. **读取魔数**：
  - 从缓冲区读取魔数并进行校验，确保数据格式正确。

5. **读取列名长度**：
  - 从缓冲区读取列名的长度。

6. **读取列名字符串**：
  - 从缓冲区读取列名字符串。

7. **读取其他字段**：
  - 依次读取列的数据类型（type）、长度（len_）、列索引（table_ind_）、可为空标志（nullable_）和唯一标志（unique_）。

8. **创建 `Column` 对象**：
  - 使用读取的数据创建新的 `Column` 对象。

9. **返回反序列化后的数据长度**：
  - 通过 `buf - p` 计算反序列化后的数据长度。

#### 4.2.5 Schema

序列化：

1. **断言检查**：
  - 确保序列化后的数据大小不超过 `PAGE_SIZE`。

2. **初始化缓冲区指针**：
  - 使用指针 `p` 保存缓冲区的起始位置。

3. **写入列的数量**：
  - 获取列的数量并写入缓冲区。

4. **写入每个列的内容**：
  - 遍历 `columns_` 容器中的每个 `Column` 对象，调用其 `SerializeTo` 方法将其序列化并写入缓冲区。

5. **返回序列化后的数据长度**：
  - 通过 `buf - p` 计算序列化后的数据长度。

反序列化：

1. **断言检查**：
  - 确保 `schema` 指针为空，以避免内存泄漏或重复初始化。

2. **初始化缓冲区指针**：
  - 使用指针 `p` 保存缓冲区的起始位置。

3. **读取列的数量**：
  - 从缓冲区读取列的数量。

4. **读取每个列的内容**：
  - 遍历读取的列数量，依次调用 `Column::DeserializeFrom` 方法，从缓冲区反序列化出每个 `Column` 对象，并将其存储在 `columns` 向量中。

5. **读取 `is_manage_` 标志**：
  - 从缓冲区读取 `is_manage_` 标志并进行布尔值转换。

6. **创建 `Schema` 对象**：
  - 使用读取的列向量和 `is_manage_` 标志创建新的 `Schema` 对象。

7. **返回反序列化后的数据长度**：
  - 通过 `buf - p` 计算反序列化后的数据长度。

#### 4.2.6 堆表的实现

​	堆表的数据结构和教材上的基本一致，由表头、空闲空间和已经插入数据三部分组成。在这部分我们需要完成的函数有：

- `TableHeap:InsertTuple(&row, *txn)`: 向堆表中插入一条记录，插入记录后生成的`RowId`需要通过`row`对象返回（即`row.rid_`）；
- `TableHeap:UpdateTuple(&new_row, &rid, *txn)`：将`RowId`为`rid`的记录`old_row`替换成新的记录`new_row`，并将`new_row`的`RowId`通过`new_row.rid_`返回；
- `TableHeap:ApplyDelete(&rid, *txn)`：从物理意义上删除这条记录；
- `TableHeap:GetTuple(*row, *txn)`：获取`RowId`为`row->rid_`的记录；
- `TableHeap:FreeHeap()`：销毁整个`TableHeap`并释放这些数据页；
- `TableHeap::Begin()`：获取堆表的首迭代器；
- `TableHeap::End()`：获取堆表的尾迭代器；
- `TableIterator`类中的成员操作符

- - `TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn)` :初始化`TableIterator`类
  - `TableIterator::TableIterator(TableHeap *table_heap, RowId &rid, Txn *txn, Row *row)`:初始化`TableIterator`类
  - `TableIterator::TableIterator(const TableIterator &other)`:初始化`TableIterator`类
  - `TableIterator::operator==(const TableIterator &itr) const`: 判断两个`TableIterator`类是否相等
  - `TableIterator::operator!=(const TableIterator &itr) const`：判断两个`TableIterator`类是否不相等
  - `const Row &TableIterator::operator*()`：指向`TableIterator`类中的`row_`内容
  - `Row *TableIterator::operator->()`：指向`TableIterator`类中的`row_`地址
  - `TableIterator::operator++()`：移动到下一条记录，通过`++iter`调用；
  - `TableIterator::operator++(int)`：移动到下一条记录，通过`iter++`调用；

#### 4.2.7 测试结果

![alt text](assets/image-2.png)

![alt text](assets/image-3.png)

### 4.3 INDEX MANAGER

#### 4.3.1 概述

Index Manager 负责数据表索引的实现和管理，包括：索引的创建和删除，索引键的等值查找，索引键的范围查找（返回对应的迭代器），以及插入和删除键值等操作，并对外提供相应的接口。

​通过遍历堆表的方式来查找一条记录是十分低效的，为了能够快速定位到某条记录而无需搜索数据表中的每一条记录，我们需要在上一个实验的基础上实现一个索引，这能够为快速随机查找和高效访问有序记录提供基础。索引有很多种实现方式，如B+树索引，Hash索引等等。在本模块中，我实现了一个基于磁盘的B+树动态索引结构。

#### 4.3.2 B+ 树数据页

#### 4.3.3 B+ 树索引

#### 4.3.4 B+ 树索引迭代器

#### 4.3.5 测试结果

![alt text](assets/image-4.png)

![alt text](assets/image-5.png)

![alt text](assets/image-6.png)

![alt text](assets/image-7.png)

### 4.4 CATALOG MANAGER

#### 4.4.1 概述

​	Catalog Manager 负责管理和维护数据库的所有模式信息，包括：

- 数据库中所有表的定义信息，包括表的名称、表中字段（列）数、主键、定义在该表上的索引。
- 表中每个字段的定义信息，包括字段类型、是否唯一等。
- 数据库中所有索引的定义，包括所属表、索引建立在那个字段上等。

#### 4.4.2 目录元信息

​	数据库中定义的表和索引在内存中以`TableInfo`和`IndexInfo`的形式表现，它们分别定义于`src/include/catalog/table.h`和`src/include/catalog/indexes.h`，其维护了与之对应的表或索引的元信息和操作对象。以`IndexInfo`为例，它包含了这个索引定义时的元信息`meta_data_`，该索引对应的表信息`table_info_`，该索引的模式信息`key_schema_`和索引操作对象`index_`。除元信息`meta_data_`外，其它的信息（如`key_schema_`、`table_info_`等）都是通过反序列化后的元信息生成的。也就是说，为了能够将所有表和索引的定义信息持久化到数据库文件并在重启时从数据库文件中恢复，我们需要为表和索引的元信息`TableMetadata`和`IndexMetadata`实现序列化和反序列化操作。它们与`TableInfo`和`IndexInfo`定义在相同文件中。在序列化时，为了简便处理，我们为每一个表和索引都分配一个单独的数据页用于存储序列化数据。因此，在这样的设计下，我们同样需要一个数据页和数据对象`CatalogMeta`（定义在`src/include/catalog/catalog.h`）来记录和管理这些表和索引的元信息被存储在哪个数据页中。`CatalogMeta`的信息将会被序列化到数据库文件的第`CATALOG_META_PAGE_ID`号数据页中（逻辑意义上），`CATALOG_META_PAGE_ID`默认值为0。

​	在序列化时，需要为每一个表和索引都分配一个单独的数据页用于存储序列化数据，因此需要用于记录和管理这些表和索引的元信息被存储在哪个数据页中的数据对象 `CatalogMeta`，它的信息将会被序列化 到数据库文件的第 `CATALOG_META_PAGE_ID `号数据页中，默认值为0。

​	反序列化时，根据写入的页数用 `MACH_READ_FROM(Type, buf) `逐个取出。

#### 4.4.3 表和索引的管理

​	`CatalogManager`类应具备维护和持久化数据库中所有表和索引的信息。`CatalogManager`能够在数据库实例（`DBStorageEngine`）初次创建时（`init = true`）初始化元数据；并在后续重新打开数据库实例时，从数据库文件中加载所有的表和索引信息，构建`TableInfo`和`IndexInfo`信息置于内存中。此外，`CatalogManager`类还需要对上层模块提供对指定数据表的操作方式，如`CreateTable`、`GetTable`、`GetTables`、`DropTable`、`GetTableIndexes`；对上层模块提供对指定索引的操作方式，如`CreateIndex`、`GetIndex`、`DropIndex`。

​	为了实现维护和持久化，在初始化`CatalogManager`时，需要调用`FlushCatalogMetaPage()`，这是为了读取之前使用数据库保存下的数据。

#### 4.4.4 测试结果

![alt text](assets/image-8.png)

### 4.5 PLANNER AND EXECUTOR

#### 4.5.1 概述

​	本模块主要包括Planner和Executor两部分。Planner的主要功能是将解释器（Parser）生成的语法树，改写成数据库可以理解的数据结构。在这个过程中，我们会将所有sql语句中的标识符（Identifier）解析成没有歧义的实体，即各种C++的类，并通过Catalog Manager 提供的信息生成执行计划。Executor遍历查询计划树，将树上的 `PlanNode `替换成对应的 Executor，随后调用 Record Manager、Index Manager 和 Catalog Manager 提供的相应接口进行执行，并将执行结果返回给上层。

#### 4.5.2 Parser生成语法树

​	由于我们尚未接触到编译原理的相关知识，在本实验中，助教已经为同学们设计好MiniSQL中的Parser模块。

​	以下是语法树（结点）的数据结构定义，每个结点都包含了一个唯一标识符`id_`，唯一标识符在调用`CreateSyntaxNode`函数时生成（框架中已经给出实现）。`type_`表示语法树结点的类型，`line_no_`和`col_no_`表示该语法树结点对应的是SQL语句的第几行第几列，`child_`和`next_`分别表示该结点的子结点和兄弟结点，`val_`用作一些额外信息的存储（如在`kNodeString`类型的结点中，`val_`将用于存储该字符串的字面量）。

```c++
/**
 * Syntax node definition used in abstract syntax tree.
 */
struct SyntaxNode {
  int id_;    /** node id for allocated syntax node, used for debug */
  SyntaxNodeType type_; /** syntax node type */
  int line_no_; /** line number of this syntax node appears in sql */
  int col_no_;  /** column number of this syntax node appears in sql */
  struct SyntaxNode *child_;  /** children of this syntax node */
  struct SyntaxNode *next_;   /** siblings of this syntax node, linked by a single linked list */
  char *val_; /** attribute value of this syntax node, use deep copy */
};
typedef struct SyntaxNode *pSyntaxNode;
```

​	Parser模块中目前能够支持以下类型的SQL语句。其中包含了一些在语法定义上正确，但在语义上错误的SQL语句（如Line 8～10）需要我们在执行器中对这些特殊情况进行处理。此外涉及到事务开启、提交和回滚相关的`begin`、`commit`和`rollback`命令可以不做实现。

```sql
create database db0;
drop database db0;
show databases;
use db0;
show tables;
create table t1(a int, b char(20) unique, c float, primary key(a, c));
create table t1(a int, b char(0) unique, c float, primary key(a, c));
create table t1(a int, b char(-5) unique, c float, primary key(a, c));
create table t1(a int, b char(3.69) unique, c float, primary key(a, c));
create table t1(a int, b char(-0.69) unique, c float, primary key(a, c));
create table student(
  sno char(8),
  sage int,
  sab float unique,
  primary key (sno, sab)
);
drop table t1;
create index idx1 on t1(a, b);
-- "btree" can be replaced with other index types
create index idx1 on t1(a, b) using btree;
drop index idx1;
show indexes;
select * from t1;
select id, name from t1;
select * from t1 where id = 1;
-- note: use left association
select * from t1 where id = 1 and name = "str";
select * from t1 where id = 1 and name = "str" or age is null and bb not null;
insert into t1 values(1, "aaa", null, 2.33);
delete from t1;
delete from t1 where id = 1 and amount = 2.33;
update t1 set c = 3;
update t1 set a = 1, b = "ccc" where b = 2.33;
begin;
commit;
rollback;
quit;
execfile "a.txt";
```

​	对于简单的语句例如show databases，drop table 等，生成的语法树也非常简单。以show databases为例，对应的语法树只有单节点kNodeShowDB，表示展示所有数据库。此时无需传入Planner生成执行计划，我们直接调用对应的执行函数执行即可。

​	对于复杂的语句，生成的语法树需传入Planner生成执行计划，并交由Executor进行执行。Planner需要先遍历语法树，调用Catalog Manager 检查语法树中的信息是否正确，如表、列是否存在，谓词的值类型是否与column类型对应等等，随后将这些词语抽象成相应的表达式（表达式在`src/include/planner/expressions/`），即可以理解的各种 c++ 类。解析完成后，Planner根据改写语法树后生成的可以理解的Statement结构，生成对应的Plannode，并将Planndoe交由executor进行执行。

#### 4.5.2 无需查询计划的功能

​    由于本次新增模块6和bonus模块，本模块的算子部分已由助教完成，我们只需要实现`src/include/executor/execute_engine.h`中的创建删除查询数据表、索引等函数。它们对应的语法树较为简单，因此不用通过Planner生成查询计划。它们被声明为`private`类型的成员，所有的执行过程对上层模块隐藏，上层模块只需要调用`ExecuteEngine::execute()`并传入语法树结点即可无感知地获取到执行结果。

​    下面是我实现的函数的思路

1. `dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context)`

- **检查当前数据库**：
  - 如果没有选择当前数据库 (`current_db_.empty()`)，输出错误信息并返回 `DB_FAILED`。

- **解析语法树节点**：
  - 从 `ast` 中获取表名和表的属性定义节点 `pAst`。

- **初始化变量**：
  - `TableInfo *table_info(nullptr)`：用于存储表的信息。
  - `std::vector<Column*> columns`：存储表的列定义。
  - `std::vector<string> unique_columns`：存储标记为 Unique 的列。
  - `std::vector<string> primary_key`：存储主键列的名称。

- **遍历属性定义节点** (`pAst`)：
  - 根据节点类型分别处理列定义和主键定义：
    - **列定义 (`kNodeColumnDefinition`)**：
      - 解析列名、类型、是否为 Unique，并创建 `Column` 对象加入 `columns`。
      - 如果列被标记为 Unique，则将列名加入 `unique_columns`。
    - **主键定义 (`kNodeColumnList`)**：
      - 遍历子节点获取每个主键列名，加入 `primary_key`。

- **创建表和索引**：
  - 创建 `Schema` 对象用于表结构的描述。
  - 获取事务对象 `Txn* transaction` 和 `CatalogManager* catalog_manager`。
  - 调用 `catalog_manager->CreateTable()` 创建表，如果表已存在返回 `DB_ALREADY_EXIST`。
  - 对于每个 Unique 列，创建对应的索引。
  - 如果存在主键列，则创建主键索引。

- **返回状态**：
  - 根据操作成功与否返回 `DB_SUCCESS` 或 `DB_ALREADY_EXIST`。

2. `dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context)`

- **检查当前数据库**：
  - 如果没有选择当前数据库 (`current_db_.empty()`)，输出错误信息并返回 `DB_FAILED`。

- **获取 `CatalogManager` 对象**：
  - 通过 `context` 获取当前数据库的 `CatalogManager`。

- **获取表名**：
  - 从 `ast` 中获取要删除的表名。

- **调用 `CatalogManager` 删除表**：
  - 调用 `catalog_manager->DropTable(table_name)` 删除指定表。

- **返回状态**：
  - 返回操作结果，成功返回 `DB_SUCCESS`，否则根据具体情况返回其他错误码。

3. `dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context)`

- **检查当前数据库**：
  - 如果没有选择当前数据库 (`current_db_.empty()`)，输出错误信息并返回 `DB_FAILED`。

- **获取 `CatalogManager` 对象**：
  - 通过 `context` 获取当前数据库的 `CatalogManager`。

- **获取所有表信息**：
  - 调用 `catalog_manager->GetTables(tables)` 获取当前数据库中所有表的信息。

- **获取每个表的索引信息**：
  - 对于每个表，调用 `catalog_manager->GetTableIndexes(table_name, indexes)` 获取该表的所有索引信息。

- **计算最大列宽度**：
  - 遍历表和索引信息，计算出最长的表名和索引名，用于格式化输出。

- **格式化输出**：
  - 使用 `setw` 和 `setfill` 函数设置输出格式，按表格形式输出表名和索引名。

- **返回状态**：
  - 成功显示索引信息返回 `DB_SUCCESS`，否则返回 `DB_FAILED`。

4. `dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context)`

- **检查当前数据库**：
  - 如果没有选择当前数据库 (`current_db_.empty()`)，输出错误信息并返回 `DB_FAILED`。

- **获取 `CatalogManager` 对象**：
  - 通过 `context` 获取当前数据库的 `CatalogManager`。

- **解析语法树节点**：
  - 从 `ast` 中获取索引名、表名、索引列名和索引类型。

- **创建索引**：
  - 调用 `catalog_manager->CreateIndex(table_name, index_name, index_keys, txn, index_info, index_type)` 创建索引。

- **返回状态**：
  - 根据操作成功与否返回 `DB_SUCCESS` 或其他错误码。

5. `dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context)`

- **检查当前数据库**：
  - 如果没有选择当前数据库 (`current_db_.empty()`)，输出错误信息并返回 `DB_FAILED`。

- **获取 `CatalogManager` 对象**：
  - 通过 `context` 获取当前数据库的 `CatalogManager`。

- **获取索引名**：
  - 从 `ast` 中获取要删除的索引名。

- **遍历所有表**：
  - 调用 `catalog_manager->GetTables(table_infos)` 获取当前数据库中所有表的信息。
  - 对于每个表，调用 `catalog_manager->GetTableIndexes(table_name, index_infos)` 获取该表的所有索引信息。

- **删除索引**：
  - 对于每个表的每个索引，如果索引名与要删除的索引名相同，则调用 `catalog_manager->DropIndex(table_name, index_name)` 删除索引。

- **返回状态**：
  - 如果成功删除至少一个索引返回 `DB_SUCCESS`，否则返回 `DB_INDEX_NOT_FOUND`。

#### 4.5.11 测试结果

![alt text](assets/image-10.png)

### 4.6 RECOVERY MANAGER

（卧槽这一部分是新开的没有可以抄的了）

#### 4.6.1 概述

#### 4.6.2 数据恢复

#### 4.6.3 测试结果

![alt text](assets/image-11.png)

## 5 项目测试

### 5.1 模块正确性测试 main_test.cpp

![alt text](assets/image-13.png)
