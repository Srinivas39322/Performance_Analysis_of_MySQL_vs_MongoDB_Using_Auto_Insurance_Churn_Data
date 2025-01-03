### Performance Analysis of MySQL and MongoDB 📊

---

#### **Introduction**
This project conducts a performance analysis of MySQL (a relational database) and MongoDB (a NoSQL database) using auto insurance churn data. The study evaluates the efficiency of both databases in executing queries related to customer segmentation, churn tracking, and correlation analysis. It explores how database structures and designs influence query execution time and overall performance, providing insights into their suitability for specific applications.

---

#### **Objective**
To compare the performance of MySQL and MongoDB in handling structured and semi-structured data, particularly focusing on queries related to customer churn in the auto insurance industry, and to provide actionable insights for selecting an optimal database system based on project needs.

---

#### **SMART Questions**
1. How does MySQL's relational structure perform compared to MongoDB's document-oriented design in terms of query execution times?
2. Which database is more efficient for handling customer segmentation and churn analysis in large datasets?
3. What are the performance differences when executing specific queries such as aggregation and correlation analysis?
4. Can MongoDB's horizontal scalability outperform MySQL's indexing strategies for analytical queries?
5. How do database-specific features (e.g., aggregation pipelines in MongoDB) impact performance in real-world applications?

---

#### **Dataset**
- **Source**: Kaggle's Auto Insurance Churn Dataset
- **Dimensions**: 500,000 rows, 16 columns
- **Key Features**:
  - Customer demographics
  - Insurance premiums
  - Churn status
  - Income and tenure

---

#### **Key Findings/Conclusion**
1. **Performance Insights**:
   - **MySQL** excelled in structured data queries, benefiting from advanced indexing and normalization techniques.
   - **MongoDB** outperformed in scenarios requiring intricate aggregations and dynamic categorization using its expressive aggregation pipeline.
   
2. **Query-Specific Observations**:
   - MongoDB demonstrated superior performance in tracking churn status over time and customer segmentation queries.
   - MySQL was more efficient for correlation analysis and handling complex joins.

3. **Use-Case Suitability**:
   - **MySQL**: Ideal for applications requiring predefined schemas, complex relational queries, and structured data.
   - **MongoDB**: Better suited for projects involving semi-structured or rapidly evolving data and real-time applications.

4. **Conclusion**:
   - The choice between MySQL and MongoDB should be driven by the nature of the dataset and project requirements. While MySQL is reliable for structured data, MongoDB provides flexibility and scalability for diverse data structures and analytical tasks.

--- 
