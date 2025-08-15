# 🎵 MuseDash — Music Streaming Analytics Dashboard  

## 📌 Overview  
MuseDash is an interactive music analytics dashboard built to analyze **historical streaming data** from Zip-pot-ify, a fictional but nationwide music platform. The project showcases our ability to design and deploy a **full data engineering and analytics pipeline** — from raw data ingestion to dynamic visualizations — highlighting **regional listening trends, artist popularity, genre breakdowns, and time-based metrics**.  

This project was developed collaboratively as a portfolio piece to demonstrate **modern data engineering and data visualization practices**.  

---

## 👩‍💻 The Team  
- **Angelika Brown** — [LinkedIn](https://www.linkedin.com/in/angelikabrown/)  
- **Isiah Armstrong**  — [LinkedIn](https://www.linkedin.com/in/isiaharmstrong00/) 
- **James Heller**  — [LinkedIn](https://www.linkedin.com/in/james-heller-xiii/) 
- **Kunle Adeyanju**  — [LinkedIn](https://www.linkedin.com/in/kunleadeyanju/) 

---

## 🚀 Tech Stack  

| Layer | Tools & Technologies |
|-------|----------------------|
| **Data Ingestion & Storage** | **AWS S3** (data storage), **AWS IAM** (security & permissions) |
| **Data Processing** | **PySpark** (distributed processing), **Pandas** (data wrangling) |
| **Visualization** | **Altair**, **Plotly** |
| **Application Layer** | **Streamlit** (interactive dashboard) |
| **Version Control & Collaboration** | **GitHub** |

---

## 🔄 Data Pipeline Architecture  

Our pipeline processes millions of rows of listening data efficiently, using a combination of **cloud storage, distributed computing, and interactive front-end visualization**.  

**Workflow:**  
1. **Data Storage:** Raw CSV files stored in AWS S3.  
2. **Data Processing:**  
   - Loaded into **PySpark** for distributed transformation & aggregation.  
   - Enriched and cleaned using **Pandas** for targeted manipulations.  
3. **Analytics:** Generated metrics such as:  
   - Most streamed artists/songs by region  
   - Genre popularity trends over time  
   - Listening activity heatmaps  
4. **Visualization:** Interactive charts and maps using **Altair** & **Plotly**.  
5. **Dashboard Deployment:** Streamlit app providing filtering, search, and drill-down capabilities.  

📌 **Pipeline Diagram:**  
![Pipeline](https://github.com/user-attachments/assets/73902ea0-59af-41b0-823e-82004129f6dd)  

---

## 📊 Dashboard Features  
- **Choropleth Maps** — visualize listening habits across U.S. states.  
- **Artist & Genre Filters** — deep dive into specific music categories.  
- **Time-based Trends** — track popularity shifts over time.  
- **Responsive Design** — fast filtering with **Streamlit caching** for smooth UI.  

---

## 💡 Portfolio Value  
This project demonstrates:  
- Building an **end-to-end data pipeline** using modern big data tools.  
- **Optimizing performance** for large datasets with PySpark.  
- Designing **interactive, user-friendly dashboards** for business insights.  
- **Team collaboration** in a realistic, production-style workflow.  
