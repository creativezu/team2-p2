# team2-p2
Team 2 Project 2 Alvin, Chris, Taiyewo

Requirements:
- Create a Spark Application that processes COVID data
- Your project 2 pitch should involve some analysis of COVID data. This is the central feature. 
- Send me a link to a git repo, have someone in the group manage git (but they can ask for help from me)
- Produce one or more .jar files for your analysis. Then you can run application using spark-submit (in Ubuntu).
- 10 queries per group
- find a trend
- implement logging (with Spark)
- graphics and visuals for results
	- Zeppelin, Tableau, Excel
- Implement Agile Scrum methodology for project work (choose Scrum Master who will serve as team lead, all communication with me funneled through this associate, and have daily scrum meetings, by end of day report blockers)

### Presentations
- Bring a simple slide deck providing an overview of your results. You should present your results, a high level overview of the process used to achieve those results, and any assumptions and simplifications you made on the way to those results.
- I may ask you to run an analysis on the day of the presentation, so be prepared to do so.
- We'll have 15 minutes per group, so make sure your presentation can be covered in that time, focusing on the parts of your analysis you find most interesting.
- Include a link to your github repository at the end of your slides

### Technologies

- Apache Spark
- Spark SQL
- YARN
- HDFS and/or S3
- SBT
- Scala 
- Git + GitHub
- Zeppelin (Other visualization tools)


### Due Date
- Presentations will take place on Thursday, 2/17

SELECT DISTINCT location, MAX(total_cases) cases FROM covid-data GROUP BY location ORDER BY cases DESC LIMIT 10