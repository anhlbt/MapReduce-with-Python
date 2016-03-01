# Lập trình MapReduce với Python

Trong bài viết này, ta sẽ thiết kế và cài đặt các thuật toán MapReduce cho các tác vụ xử lý dữ liệu thông thường. Mô hình lập trình MapReduce được đề xuất trong một bài báo năm 2004 từ một nhóm nghiên cứu tại Google. MapReduce là một mô hình đơn giản để xử lý song song các tập dữ liệu lớn (Big Data).

Bài viết này giúp bạn làm quen với tư duy lập trình MapReduce. Ta sẽ sử dụng tập dữ liệu nhỏ để dễ kiểm tra kết quả thực thi cũng như để quan sát hoạt động bên trong MapReduce như thế nào.

# Giới thiệu về tập dữ liệu

**books.json**

Đây là tập dữ liệu chứa danh sách các văn bản. Mỗi dòng tương ứng với một văn bản gồm document_id và text. Quan sát 3 dòng đầu tiên của tập dữ liệu.

```
["milton-paradise.txt", "[ Paradise Lost by John Milton 1667 ] Book I Of Man ' s first disobedience , and the fruit Of that forbidden tree whose mortal taste Brought death into the World , and all our woe , With loss of Eden , till one greater Man Restore us , and regain the blissful seat , Sing , Heavenly Muse , that , on the secret top Of Oreb , or of Sinai , didst inspire That shepherd who first taught the chosen seed In the beginning how the heavens and earth Rose out of Chaos : or , if Sion hill Delight thee more , and Siloa ' s brook that flowed Fast by the oracle of God , I thence Invoke thy aid to my adventurous song , That with no middle flight intends to soar Above th ' Aonian mount , while it pursues Things unattempted yet in prose or rhyme ."]
["edgeworth-parents.txt", "[ The Parent ' s Assistant , by Maria Edgeworth ] THE ORPHANS . Near the ruins of the castle of Rossmore , in Ireland , is a small cabin , in which there once lived a widow and her four children . As long as she was able to work , she was very industrious , and was accounted the best spinner in the parish ; but she overworked herself at last , and fell ill , so that she could not sit to her wheel as she used to do , and was obliged to give it up to her eldest daughter , Mary ."]
["austen-emma.txt", "[ Emma by Jane Austen 1816 ] VOLUME I CHAPTER I Emma Woodhouse , handsome , clever , and rich , with a comfortable home and happy disposition , seemed to unite some of the best blessings of existence ; and had lived nearly twenty - one years in the world with very little to distress or vex her . She was the youngest of the two daughters of a most affectionate , indulgent father ; and had , in consequence of her sister ' s marriage , been mistress of his house from a very early period . Her mother had died too long ago for her to have more than an indistinct remembrance of her caresses ; and her place had been supplied by an excellent woman as governess , who had fallen little short of a mother in affection ."]
...
```

**records.json**

Đây là tập dữ liệu chứa thông tin các đơn hàng gồm Order và LineItem. Order chứa thông tin về một đơn hàng, LineItem chứa thông tin các sản phẩm thuộc về một đơn hàng. Quan sát các dòng dữ liệu thuộc về đơn hàng có id=1.

```
["order", "1", "36901", "O", "173665.47", "1996-01-02", "5-LOW", "Clerk#000000951", "0", "nstructions sleep furiously among "]
["line_item", "1", "155190", "7706", "1", "17", "21168.23", "0.04", "0.02", "N", "O", "1996-03-13", "1996-02-12", "1996-03-22", "DELIVER IN PERSON", "TRUCK", "egular courts above the"]
["line_item", "1", "67310", "7311", "2", "36", "45983.16", "0.09", "0.06", "N", "O", "1996-04-12", "1996-02-28", "1996-04-20", "TAKE BACK RETURN", "MAIL", "ly final dependencies: slyly bold "]
["line_item", "1", "63700", "3701", "3", "8", "13309.60", "0.10", "0.02", "N", "O", "1996-01-29", "1996-03-05", "1996-01-31", "TAKE BACK RETURN", "REG AIR", "riously. regular, express dep"]
...
```

**friends.json**

Đây là tập dữ liệu đơn giản về mối quan hệ bạn bè giữa các user trên mạng xã hội. Mỗi dòng tương ứng với cặp giá trị person và friend. Quan sát 3 dòng đầu tiên của tập dữ liệu.

```
["Myriel", "Valjean"]
["Napoleon", "Myriel"]
["MlleBaptistine", "Myriel"]
...
```

**dna.json**

Đây là tập dữ liệu về chuỗi DNA. Mỗi dòng tương ứng cặp giá trị sequence id và nucleotides. Quan sát 3 dòng đầu tiên của tập dữ liệu.

```
["RATGRG", "GGGGTGGCTACCCAGAGGCATGCTCCTCACCCAGCTCCACTGTCCCTACCTGCTGCTGCTGCTGGTGGTGCTGTCATGTCTGGTGAGTGCCGTGCACCCCACAGCACCTGCATGGAGGAGGGTTGGCTGCTCTGTACACAAGTGCTGAGAGCTCTCTGGTTGCTTGCCTACCTGTTTCCCAGCCAAAGGCACCCTCTGCCCAGGTAATGGACTTTTTGTTTGAGAAGTGGAAGCTCTATAGTGACCAGTGCCACCACAACCTAAGCCT..."]
["RATOSCAL", "AAGAACAACCTTCACTTTAATATTATTGATAACATTTAGTTTCTGGATATCAAGTGGGCTCCTACTAGATCATGCCAGGTCACCAAATACCAGTTTAATCAAGGAAGGAAGAAAGAAAAGGAACAGTAAATAAAAAGAGTGACAAGTACTAAGAATTGCAAAAAGGACATCCCAAGGTGTTTGGACCAAAGAAGCGGCCTTGGGGCCTCTCAGTACTCATACTGGGCCCCAAGAGACCATGGCCATTGCTCTGAAATACGGTAACCCGGCAGGTTTTTCTTCCTTGTCATCAGGGGTCCCAGGCATCTTGAGCTTCATGTGGGGTGTCTCTGACACAAGCAGGGCTAGAACC..."]
["HUMGHN", "GAATTCAGGACTGAATCGTGCTCACAACCCCCACAATCTATTGGCTGTGCTTGGCCCCTTTTCCCAACACACACATTCTGTCTGGTGGGTGGAGGTTAAACATGCGGGGAGGAGGAAAGGGATAGGATAGAGAATGGGATGTGGTCGGTAGGGGGTCTCAAGGACTGGCCTATCCTGACATCCTTCGCCCGCGTGCAGGTTGGCCACCATGGCCTGCGGCCAGAGGGCACCCACGTGACCCTTAAAGAGAGGACAAGTTGGGTGGTATCTCTGGCTGACACTCTGTGCACAACCCTCACAACACTGGTGACGGTGGGAAGGGAAAGATG..."]
...
```

**matrix.json**

Đây là tập dữ liệu chứa các giá trị của hai ma trận a và b. Mỗi dòng gồm các thông tin [matrix, i, j, value]. Trong đó, matrix là tên của ma trận (a/b). Quan sát một vài dòng của tập dữ liệu.

```
["a", 3, 4, 18]
["a", 4, 0, 7]
["a", 4, 1, 98]
["a", 4, 2, 96]
["a", 4, 3, 27]
["b", 0, 0, 63]
["b", 0, 1, 18]
["b", 0, 2, 89]
["b", 0, 3, 28]
...
```

# Tính term-frequence
**Bài toán:** cho tập dữ liệu book.json, term-frequency là tần suất xuất hiện của một từ trong các văn bản.

**Map input:**

```map (in_key, in_value) -> list(out_key, intermediate_value)```
- in_key: “all”
- in_value: 1 > 1 > 1 > 1
- out_key: “all”
- intermediate_value: [1, 1, 1, 1]

**Reduce output (wordcount.json):**

```reduce (out_key, list(intermediate_value)) -> list(out_value)```
- out_key: “all”
- intermediate_value: [1, 1, 1, 1]
- out_value: (“all”, 4)

```
["all", 4]
["Rossmore", 1]
["Consumptive", 1]
...
```

**Cài đặt:** wordcount.py

# Tính Inverted index

**Bài toán:** cho tập dữ liệu book.json, inverted index là một dictionary trong đó mỗi từ liên kết với danh sách các văn bản mà nó xuất hiện.

**Map input:**

```map (in_key, in_value) -> list(out_key, intermediate_value)```
- in_key: “all”
- in_value: “milton-paradise.txt” > “blake-poems.txt” > “melville-moby_dick.txt”
- out_key: “all”
- intermediate_value: [“milton-paradise.txt”, “blake-poems.txt”, “melville-moby_dick.txt”]

**Reduce output (inverted_index.json):**

```reduce (out_key, list(intermediate_value)) -> list(out_value)```
- out_key: “all”
- intermediate_value: [“milton-paradise.txt”, “blake-poems.txt”, “melville-moby_dick.txt”]
- out_value: (“all”, [“milton-paradise.txt”, “blake-poems.txt”, “melville-moby_dick.txt”])

```
["all", ["milton-paradise.txt", "blake-poems.txt", "melville-moby_dick.txt"]]
["Rossmore", ["edgeworth-parents.txt"]]
["Consumptive", ["melville-moby_dick.txt"]]
...
```

**Cài đặt:** inverted_index.py

# Kết bảng thông tin đơn hàng

**Bài toán:** cho tập dữ liệu records.json. Quan sát câu truy vấn SQL dưới đây

```
SELECT *
FROM Orders, LineItem
WHERE Order.order_id = LineItem.order_id
```

Chúng ta sẽ thiết kế giải thuật MapReduce sao cho có kết quả trả về tương tự như câu lệnh SQL trên.

**Map input:**

```map (in_key, in_value) -> list(out_key, intermediate_value)```
- in_key: 32
- in_value: [“order“, “32”, “130057”, “O”, “208660.75”, “1995-07-16”, “2-HIGH”, “Clerk#000000616”, “0”, “ise blithely bold, regular requests. quickly unusual dep”] >
[“line_item“, “32”, “82704”, “7721”, “1”, “28”, “47227.60”, “0.05”, “0.08”, “N”, “O”, “1995-10-23”, “1995-08-27”, “1995-10-26”, “TAKE BACK RETURN”, “TRUCK”, “sleep quickly. req”] >
[“line_item“, “32”, “197921”, “441”, “2”, “32”, “64605.44”, “0.02”, “0.00”, “N”, “O”, “1995-08-14”, “1995-10-07”, “1995-08-27”, “COLLECT COD”, “AIR”, “lithely regular deposits. fluffily “] >
[“line_item“, “32”, “44161”, “6666”, “3”, “2”, “2210.32”, “0.09”, “0.02”, “N”, “O”, “1995-08-07”, “1995-10-07”, “1995-08-23”, “DELIVER IN PERSON”, “AIR”, ” express accounts wake according to the”]
- out_key: 32
- intermediate_value: [[“order“, “32”, “130057”, “O”, “208660.75”, “1995-07-16”, “2-HIGH”, “Clerk#000000616”, “0”, “ise blithely bold, regular requests. quickly unusual dep”] >
[“line_item“, “32”, “82704”, “7721”, “1”, “28”, “47227.60”, “0.05”, “0.08”, “N”, “O”, “1995-10-23”, “1995-08-27”, “1995-10-26”, “TAKE BACK RETURN”, “TRUCK”, “sleep quickly. req”] >
[“line_item“, “32”, “197921”, “441”, “2”, “32”, “64605.44”, “0.02”, “0.00”, “N”, “O”, “1995-08-14”, “1995-10-07”, “1995-08-27”, “COLLECT COD”, “AIR”, “lithely regular deposits. fluffily “] >
[“line_item“, “32”, “44161”, “6666”, “3”, “2”, “2210.32”, “0.09”, “0.02”, “N”, “O”, “1995-08-07”, “1995-10-07”, “1995-08-23”, “DELIVER IN PERSON”, “AIR”, ” express accounts wake according to the”]
]

**Reduce output (join.json):**

```reduce (out_key, list(intermediate_value)) -> list(out_value)```
- out_key: 32
- intermediate_value: [[“order“, “32”, “130057”, “O”, “208660.75”, “1995-07-16”, “2-HIGH”, “Clerk#000000616”, “0”, “ise blithely bold, regular requests. quickly unusual dep”] >
[“line_item“, “32”, “82704”, “7721”, “1”, “28”, “47227.60”, “0.05”, “0.08”, “N”, “O”, “1995-10-23”, “1995-08-27”, “1995-10-26”, “TAKE BACK RETURN”, “TRUCK”, “sleep quickly. req”] >
[“line_item“, “32”, “197921”, “441”, “2”, “32”, “64605.44”, “0.02”, “0.00”, “N”, “O”, “1995-08-14”, “1995-10-07”, “1995-08-27”, “COLLECT COD”, “AIR”, “lithely regular deposits. fluffily “] >
[“line_item“, “32”, “44161”, “6666”, “3”, “2”, “2210.32”, “0.09”, “0.02”, “N”, “O”, “1995-08-07”, “1995-10-07”, “1995-08-23”, “DELIVER IN PERSON”, “AIR”, ” express accounts wake according to the”]
]
- out_value: [“order“, “32“, “130057”, “O”, “208660.75”, “1995-07-16”, “2-HIGH”, “Clerk#000000616”, “0”, “ise blithely bold, regular requests. quickly unusual dep”, “line_item“, “32“, “82704”, “7721”, “1”, “28”, “47227.60”, “0.05”, “0.08”, “N”, “O”, “1995-10-23”, “1995-08-27”, “1995-10-26”, “TAKE BACK RETURN”, “TRUCK”, “sleep quickly. req”]
[“order“, “32“, “130057”, “O”, “208660.75”, “1995-07-16”, “2-HIGH”, “Clerk#000000616”, “0”, “ise blithely bold, regular requests. quickly unusual dep”, “line_item“, “32“, “197921”, “441”, “2”, “32”, “64605.44”, “0.02”, “0.00”, “N”, “O”, “1995-08-14”, “1995-10-07”, “1995-08-27”, “COLLECT COD”, “AIR”, “lithely regular deposits. fluffily “]
[“order“, “32“, “130057”, “O”, “208660.75”, “1995-07-16”, “2-HIGH”, “Clerk#000000616”, “0”, “ise blithely bold, regular requests. quickly unusual dep”, “line_item“, “32“, “44161”, “6666”, “3”, “2”, “2210.32”, “0.09”, “0.02”, “N”, “O”, “1995-08-07”, “1995-10-07”, “1995-08-23”, “DELIVER IN PERSON”, “AIR”, ” express accounts wake according to the”]

```
["order", "32", "130057", "O", "208660.75", "1995-07-16", "2-HIGH", "Clerk#000000616", "0", "ise blithely bold, regular requests. quickly unusual dep", "line_item", "32", "82704", "7721", "1", "28", "47227.60", "0.05", "0.08", "N", "O", "1995-10-23", "1995-08-27", "1995-10-26", "TAKE BACK RETURN", "TRUCK", "sleep quickly. req"]
["order", "32", "130057", "O", "208660.75", "1995-07-16", "2-HIGH", "Clerk#000000616", "0", "ise blithely bold, regular requests. quickly unusual dep", "line_item", "32", "197921", "441", "2", "32", "64605.44", "0.02", "0.00", "N", "O", "1995-08-14", "1995-10-07", "1995-08-27", "COLLECT COD", "AIR", "lithely regular deposits. fluffily "]
["order", "32", "130057", "O", "208660.75", "1995-07-16", "2-HIGH", "Clerk#000000616", "0", "ise blithely bold, regular requests. quickly unusual dep", "line_item", "32", "44161", "6666", "3", "2", "2210.32", "0.09", "0.02", "N", "O", "1995-08-07", "1995-10-07", "1995-08-23", "DELIVER IN PERSON", "AIR", " express accounts wake according to the"]
...
```

**Cài đặt:** join.py

# Đếm số lượng bạn bè

**Bài toán:** cho tập dữ liệu friends.json gồm các cặp giá trị key-value (person, friend) thể hiện mối quan hệ bạn bè giữa hai người. Ta sẽ thiết kế giải thuật MapReduce để đếm số lượng bạn bè của mỗi person trong tập dữ liệu.

**Map input:**

```map (in_key, in_value) -> list(out_key, intermediate_value)```
- in_key: “MlleBaptistine”
- in_value: 1 > 1 > 1
- out_key: “MlleBaptistine”
- intermediate_value: [1, 1, 1]

**Reduce output (friend_count.json):**

```reduce (out_key, list(intermediate_value)) -> list(out_value)```
- out_key: “MlleBaptistine”
- intermediate_value: [1, 1, 1]
- out_value: (“MlleBaptistine”, 3)

```
["MlleBaptistine", 3]
["Myriel", 5]
["Valjean", 16]
...
```

**Cài đặt:** friend_count.py

# Liệt kê danh sách quan hệ bạn bè một chiều

**Bài toán:** mối quan hệ “bạn bè” thường tồn tại hai chiều. Nghĩa là, nếu tôi là bạn của bạn thì bạn cũng là bạn của tôi. Tuy nhiên, tập dữ liệu ban đầu friends.json tồn tại một số quan hệ một chiều. Ta cần thiết kế giải thuật MapReduce để kiểm tra mối quan hệ này và xuất ra danh sách các mối quan hệ bạn bè một chiều.

**Map input:**

```map (in_key, in_value) -> list(out_key, intermediate_value)```
- in_key: “MlleBaptistine”
- in_value: “Valjean”
- out_key: [“MlleBaptistine”, “Valjean”], [“Valjean”, “MlleBaptistine”]
- intermediate_value: 1, 1

**Reduce output (asymmetric_friendships.json):**

```reduce (out_key, list(intermediate_value)) -> list(out_value)```
- out_key: [“MlleBaptistine”, “Valjean”], [“Valjean”, “MlleBaptistine”]
- intermediate_value: 1, 1
- out_value: [“MlleBaptistine”, “Valjean”], [“Valjean”, “MlleBaptistine”]

```
["MlleBaptistine", "Valjean"]
["Valjean", "MlleBaptistine"]
["Fantine", "Valjean"]
["Cosette", "Valjean"]
...
```

**Cài đặt:** asymmetric_friendships.py

# Rút gọn chuỗi DNA

**Bài toán:** cho tập dữ liệu dna.json a gồm cặp giá trị key-value. Trong đó, key là sequence id, value là chuỗi nucleotides (GCTTCCGAAATGCTCGAA….). Ta thiết kết giải thuật MapReduce với điều kiện loại bỏ 10 kí tự cuối chuỗi và loại bỏ những chuỗi DNA trùng nhau.

**Map input:**

```map (in_key, in_value) -> list(out_key, intermediate_value)```
- in_key: trimmed = record[1][:-10]
- in_value: 0
- out_key: trimmed
- intermediate_value: (trimmed, 0)

**Reduce output (unique_trims.json):**

```reduce (out_key, list(intermediate_value)) -> list(out_value)```
- out_key: trimmed
- intermediate_value: (trimmed, 0)
- out_value: trimmed

```
"CTGCAGCCACCCCCTGCTGCCCCCACCTGAACCCTTGATCCCAGCTCGGCAGCCCCCGCAGTTTCCTGTTTGCCCACTCTCTTTGCCCAGCCTCAGGAACAGAGCTGATCCTTGAACTCTAAGTTCCACATCGCCAGCAAAAGTAAGCAGTGGCAGGGCCAGGCTGAGCTTATCAGTCTCCCAAGTCC..."
"CCATGGGTTGGCCAGCCTTGCCTTGACCAATAGCTTTGACAAGGCAACCTTGACCAATAGTCTTAGAGTATCGGGTGAGGCCCGGGGGCCGGTGGGTGGCTAGGGATGAAGAATAAAAGGAAGCACCCTCCATCAGTTCCACATACTCGCTCTGAAACGTCTGAGATTATCAATAAGCTCCTTGTCCAGACGCCA..."
"GAATTCACAAGCCTTTTCTCTGAGAGAGGCCTTGGGACTAGGAACTTTTTGAATGAGTGTAGAAGTCGGGAAGGAGACAATAGTGTCAACTTGGGATTGCCTAAGGCAACAACAGAGCAAAACAAGAACGCTTTGGTTCTCTGGGTCTCTGTCCCTGATTGCATAGCGGGTCATTGTTGGGAAA..."
...
```

**Cài đặt:** unique_trims.py

# Nhân hai ma trận rời rạc

**Bài toán:** giả sử ta có hai ma trận rời rạc A và B được lưu trong file matrix.json với mỗi dòng có dạng i, j, value. Ta cần thiết kết giải thuật MapReduce để nhân hai ma trận A x B.

**Map input:**

```map (in_key, in_value) -> list(out_key, intermediate_value)```
- in_key: a
- in_value: [“a”, 0, 0, 63] >
[“a”, 0, 1, 45] >
[“a”, 0, 2, 93] >
[“a”, 0, 3, 32] >
[“a”, 0, 4, 49]
...
- out_key: a
- intermediate_value: [[“a”, 0, 0, 63],
[“a”, 0, 1, 45],
[“a”, 0, 2, 93],
[“a”, 0, 3, 32],
[“a”, 0, 4, 49],
...
]

**Reduce output (multiply.json):**

```reduce (out_key, list(intermediate_value)) -> list(out_value)```
- out_key: a
- intermediate_value: [[“a”, 0, 0, 63],
[“a”, 0, 1, 45],
[“a”, 0, 2, 93],
[“a”, 0, 3, 32],
[“a”, 0, 4, 49],
...
]
- out_value: [0, 0, 11878]
[0, 1, 14044]
[0, 2, 16031]
...

```
[0, 0, 11878]
[0, 1, 14044]
[0, 2, 16031]
...
```

**Cài đặt:** multiply.py


