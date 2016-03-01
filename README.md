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
