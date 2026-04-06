# Expected Chatbot Questions (OpenTrace RAG)

The RAG is designed to handle **analytical, agriculture and food-security questions**. **BigQuery retrieval uses the bronze dataset only**; vector retrieval may still surface docs that describe silver/gold or other layers. The following are representative of what users will ask.

## Themes

- **Regional & spatial**: regions, districts, agroecological zones, national vs subnational
- **Crop productivity**: yields, yield gaps, staple crops, crop diversification, cultivated area
- **Climate & risk**: rainfall, drought, extreme weather, irrigation vs rainfed
- **Time trends**: changes over the past decade/five years, seasonal patterns, volatility
- **Food supply & markets**: food supply stability, prices, market integration, storage
- **Livelihoods**: rural employment, household incomes, commercialization
- **Vulnerability & resilience**: drought shocks, crop failure, climate stress, soil productivity

## Sample Questions (11–50)

11. Which regions have experienced the most significant changes in crop productivity over the past decade?
12. How does crop production vary between agroecological zones nationally?
13. Which crops contribute the most to national food supply stability?
14. How have rainfall patterns shifted across agricultural regions over the past ten years?
15. Where are irrigation systems having the most measurable impact on productivity?
16. Which districts show the highest concentration of staple crop production?
17. How do production trends compare between smallholder-dominated regions and commercial farming areas?
18. Which regions show persistent yield gaps compared to national averages?
19. How do agricultural production trends align with population growth patterns?
20. Where do we observe recurring crop failure events over time?
21. Which areas show increasing dependence on a single staple crop?
22. How does agricultural production correlate with rural employment patterns?
23. Which regions show the strongest link between rainfall variability and yield variability?
24. How have fertilizer usage patterns changed across regions over time?
25. Which districts appear most vulnerable to drought-related production shocks?
26. How have market price signals influenced crop planting patterns nationally?
27. Where do we observe the strongest growth in cultivated land area?
28. Which crops show the largest expansion in cultivation across regions?
29. How does agricultural productivity differ between irrigated schemes and rainfed systems?
30. Which districts demonstrate consistently higher agricultural performance over time?
31. How does agricultural production correlate with food price inflation trends?
32. Which regions show the greatest potential for improving productivity based on historical yield gaps?
33. Where do climate indicators suggest increasing exposure to extreme weather events?
34. Which areas show declining soil productivity indicators over time?
35. How does crop diversification vary across regions?
36. Which crops contribute most significantly to rural household incomes nationally?
37. Where are storage capacity constraints affecting agricultural supply patterns?
38. How do regional production patterns influence national food supply stability?
39. Which regions show increasing agricultural commercialization trends?
40. How does agricultural production fluctuate across different seasons nationally?
41. Which crops show the highest resilience to rainfall variability?
42. Where are agricultural productivity improvements most visible over the past five years?
43. Which regions show declining cultivated land despite population growth?
44. How have export-oriented crops evolved over time nationally?
45. Which regions appear most exposed to future climate stress based on historical patterns?
46. Where do agricultural and nutrition indicators show the strongest relationships?
47. How do market infrastructure investments correlate with regional agricultural performance?
48. Which regions show strong production growth but limited market integration?
49. Where do we see increasing volatility in agricultural output?
50. Which agricultural patterns should be monitored most closely in the coming seasons?

## Implications for the RAG

- **BQ retriever (NL-to-SQL)**: Schema and prompts are tuned for tables/columns about yields, regions, crops, time, rainfall, irrigation, districts, and food security. The LLM should prefer aggregations (e.g. by region, district, year) and filters on time/place when the question asks for trends or comparisons.
- **Generator**: Answers should be grounded in retrieved BigQuery rows and vector context. When data cannot fully answer the question, say so and summarize what the data does show.
- **Vector store**: Populate with docs/summaries that use the same vocabulary (regions, agroecological zones, staple crops, yield gaps, etc.) so semantic retrieval surfaces relevant context.
