## Reviewer Instructions

If you are reviewing this submission, then you can do so in two ways

* Look at the changes in [this pull request](https://github.com/equalexperts-assignments/equal-experts-abundant-substantial-spirited-photo-650b37b53f5c/pull/1)
* Browse the code on Github

## ⚠️ Please read these instructions carefully and entirely first

    ## Instructions for the reviewer

    > * You will see a pull request for candidate's solution to Vote Outliers.
    > * You can access the problem statement in the README of this repository.
    > * You can review the code as you would do it at work.
    > * All comments on the pull request will be linked to the assignment.
    > * Once you are done reviewing, Please use the link in the PR to submit your feedback.
    ------

## ⚠️ Please read these instructions carefully and entirely first

=======

>>>>>>> 065030b35bddb2714cd680bb15879db29961cc45
>>>>>>>
>>>>>>
>>>>>
>>>>
>>>
>>

* Clone this repository to your local machine.
* Use your IDE of choice to complete the assignment.
* When you have completed the assignment, you need to  push your code to this repository and [mark the assignment as completed by clicking here](https://app.snapcode.review/submission_links/fd3c2dd4-2221-4af3-a996-bbebcf5def64).
* Once you mark it as completed, your access to this repository will be revoked. Please make sure that you have completed the assignment and pushed all code from your local machine to this repository before you click the link.

**Table of Contents**

1. [Before you start](#before-you-start), a brief explanation for the exercise and software prerequisites/setup.
2. [Tips for what we are looking for](#tips-on-what-were-looking-for) provides clear guidance on solution qualities we value
3. [The Challenge](#begin-the-two-part-challenge) explains the data engineering code challenge to be tackled.
4. [Follow-up Questions](#follow-up-questions) related to the challenge which you should address
5. [Your approach and answers to follow-up questions](#your-approach-and-answers-to-follow-up-questions-) is where you should include the answers to the follow-up question and clarify your solution approach any assumptions you have made.

## Before you start

### Why complete this task?

We want to make the interview process as simple and stress-free as possible. That’s why we ask you to complete
the first stage of the process from the comfort of your own home.

Your submission will help us to learn about your skills and approach. If we think you’re a good fit for our
network, we’ll use your submission in the next interview stages too.

### About the task

You’ll be creating an ingestion process to ingest files containing vote data. You’ll also create a means to
query the ingested data to determine outlier weeks.

There’s no time limit for this task, but we expect it to take less than 2 hours.

### Software prerequisites

To make it really easy for candidates to do this exercise regardless of their operating system, we've provided a containerised
way to run the exercise. To make use of this, you'll need to have [Docker](https://www.docker.com/products/docker-desktop/) (or a free equivalent like [Rancher](https://rancherdesktop.io/) or [Colima](https://github.com/abiosoft/colima)) installed
on your system.

To start the process, there is a `Dockerfile` in the root of the project.  This defines a linux-based container that includes Python 3.11,
Poetry and DuckDB.

To build the container:

```shell
docker build -t ee-data-engineering-challenge:0.0.1 .
```

To run the container after building it:

_Mac or Linux, or Windows with WSL:_

```shell
docker run --mount type=bind,source="$(pwd)/",target=/home/dataeng -it ee-data-engineering-challenge:0.0.1 bash
```

_Windows (without WSL):_

```shell
docker run --mount type=bind,source="%cd%",target=/home/dataeng -it ee-data-engineering-challenge:0.0.1 bash
```

Running the container opens up a terminal in which you can run the poetry commands that we describe next.

You could also proceed without the container by having Python 3.11 and [Poetry](https://python-poetry.org/)
directly installed on your system. In this case, just run the poetry commands you see described here directly in your
own terminal.

### Poetry

This exercise has been written in Python 3.11 and uses [Poetry](https://python-poetry.org/) as a dependency manager.
If you're unfamiliar with Poetry, don't worry -- the only things you need to know are:

1. Poetry automatically updates the `pyproject.toml` file with descriptions of your project's dependencies to keep
   track of what libraries are being used.
2. To add a dependency for your project, use `poetry add thelibraryname`
3. To add a "dev" dependency (e.g. a test library or linter rather than something your program depends on)
   use `poetry add --dev thelibraryname`
4. To resolve dependencies and find compatible versions, use `poetry lock`
5. _Please commit any changes to your `pyproject.toml` and `poetry.lock` files and include them in your submission_
   so that we can replicate your environment.

In the terminal (of the running docker image), start off by installing the dependencies:

```shell
poetry install --with dev
```

> **Warning**
> If you're a Mac M1/M2 (arm-based) user, note that
> as of June 2023, DuckDB doesn't release pre-built `aarch64` linux
> wheels for the Python `duckdb` library (yet). This
> means that the dependency installation in the running container can take
> some time (10mins?) as it compiles `duckdb` from source.
> If you don't feel like waiting, you can build an `amd64` Docker container with
> by adding the `--platform amd64` flag to both the `docker build` and
> `docker run` commands above (i.e. you'll have to re-run those).
> This image will run seamlessly on your Mac in emulation mode.

Now type

```shell
poetry run exercise --help
```

to see the options in CLI utility we provide to help you run the exercise. For example, you could do

```shell
poetry run exercise ingest-data
```

to run the ingestion process you will write in `ingest.py`

### Bootstrap solution

This repository contains a bootstrap solution that you can use to build upon.

You can make any changes you like, as long as the solution can still be executed using the `exercise.py` script.

The base solution uses [DuckDB](https://duckdb.org/) as the database, and for your solution we want you to treat it like a real (OLAP) data warehouse.
The database should be saved in the root folder of the project on the local disk as `warehouse.db`, as shown in the `tests/db_test.py` file.

We also provide the structure for:

* `equalexperts_dataeng_exercise/ingest.py` -  the entry point for running the ingestion process.
* `equalexperts_dataeng_exercise/outliers.py` - the entry point for running the outlier detection query.
* `equalexperts_dataeng_exercise/db.py` - is empty, but the associated test demonstrates interaction with an DuckDB database.

### Tips on what we’re looking for

1. **Test coverage**

   Your solution must have good test coverage, including common execution paths.
2. **Self-contained tests**

   Your tests should be self-contained, with no dependency on being run in a specific order.
3. **Simplicity**

   We value simplicity as an architectural virtue and a development practice. Solutions should reflect the difficulty of the assigned task, and shouldn’t be overly complex. We prefer simple, well tested solutions over clever solutions.

   Please avoid:

   * unnecessary layers of abstraction
   * patterns
   * custom test frameworks
   * architectural features that aren’t called for
   * libraries like `pandas` or `polars` or frameworks like `PySpark` or `ballista` - we know that this exercise can be
     solved fairly trivially using these libraries and a Dataframe approach, and we'd encourage appropriate
     use of these in daily work contexts. But for this small exercise we really
     want to know more about how you structure, write and test your Python code,
     and want you to show some fluency in SQL -- a `pandas`
     solution won't allow us to see much of that.
4. **Self-explanatory code**

   The solution you produce must speak for itself. Multiple paragraphs explaining the solution is a sign
   that the code isn’t straightforward enough to understand on its own.
   However, please do explain your non-obvious _choices_ e.g. perhaps why you decided to load
   data a specific way.
5. **Demonstrate fluency with data engineering concepts**

   Even though this is a toy exercise, treat DuckDB as you would an OLAP
   data warehouse. Choose datatypes, data loading methods, optimisations and data models that
   are suited for resilient analytics processing at scale, not transaction processing.
6. **Dealing with ambiguity**

   If there’s any ambiguity, please add this in a section at the bottom of the README.
   You should also make a choice to resolve the ambiguity and proceed.

Our review process starts with a very simplistic test set in the `tests/exercise_tests` folder which you should also
check before submission. You can run these with:

```shell
poetry run exercise check-ingestion
poetry run exercise check-outliers
```

Expect these to fail until you have completed the exercise.

You should not change the `tests/exercise-tests` folder and your solution should be able to pass both tests.

## Download the dataset for the exercise

Run the command

```
poetry run exercise fetch-data
```

which will fetch the dataset, uncompress it and place it in `uncommitted/votes.jsonl` for you.
Explore the data to see what values and fields it contains (no need to show how you explored it).

## Begin the two-part challenge

There are two parts to the exercise, and you are expected to complete both.
A user should be able to execute each task independently of the other.
For example, ingestion shouldn't cause the outliers query to be executed.

### Part 1: Ingestion

Create a schema called `blog_analysis`.
Create an ingestion process that can be run on demand to ingest files containing vote data.
You should ensure that data scientists, who will be consumers of the data, do not need to consider
duplicate records in their queries. The data should be stored in a table called `votes` in the `blog_analysis` schema.

### Part 2: Outliers calculation

Create a view named `outlier_weeks`  in the `blog_analysis` schema. It will contain the output of a SQL calculation for which weeks are regarded as outliers based on the vote data that was ingested.
The view should contain the year, week number and the number of votes for the week _for only those weeks which are determined to be outliers_, according to the following rule:

NB! If you're viewing this Markdown document in a viewer
where the math isn't rendering, try viewing this README in GitHub on your web browser, or [see this pdf](docs/calculating_outliers.pdf).

> **A week is classified as an outlier when the total votes for the week deviate from the average votes per week for the complete dataset by more than 20%.**`</br>`For the avoidance of doubt, _please use the following formula_:
>
>> Say the mean votes is given by $\bar{x}$ and this specific week's votes is given by $x_i$.
>> We want to know when $x_i$ differs from $\bar{x}$ by more than $20$%.
>> When this is true, then the ratio $\frac{x_i}{\bar{x}}$ must be further from $1$ by more than $0.2$, i.e.: `</br></br>`
>> $\big|1 - \frac{x_i}{\bar{x}}\big| > 0.2$
>>

The data should be sorted in the view by year and week number, with the earliest week first.

Running `outliers.py` should recreate the view and
just print the contents of this `outlier_weeks` view to the terminal - don't do any more calculations after creating the view.

## Example

The sample dataset below is included in the test-resources folder and can be used when creating your tests.

Assuming a file is ingested containing the following entries:

```
{"Id":"1","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-02T00:00:00.000"}
{"Id":"2","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-09T00:00:00.000"}
{"Id":"4","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-09T00:00:00.000"}
{"Id":"5","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-09T00:00:00.000"}
{"Id":"6","PostId":"5","VoteTypeId":"3","CreationDate":"2022-01-16T00:00:00.000"}
{"Id":"7","PostId":"3","VoteTypeId":"2","CreationDate":"2022-01-16T00:00:00.000"}
{"Id":"8","PostId":"4","VoteTypeId":"2","CreationDate":"2022-01-16T00:00:00.000"}
{"Id":"9","PostId":"2","VoteTypeId":"2","CreationDate":"2022-01-23T00:00:00.000"}
{"Id":"10","PostId":"2","VoteTypeId":"2","CreationDate":"2022-01-23T00:00:00.000"}
{"Id":"11","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-30T00:00:00.000"}
{"Id":"12","PostId":"5","VoteTypeId":"2","CreationDate":"2022-01-30T00:00:00.000"}
{"Id":"13","PostId":"8","VoteTypeId":"2","CreationDate":"2022-02-06T00:00:00.000"}
{"Id":"14","PostId":"13","VoteTypeId":"3","CreationDate":"2022-02-13T00:00:00.000"}
{"Id":"15","PostId":"13","VoteTypeId":"3","CreationDate":"2022-02-20T00:00:00.000"}
{"Id":"16","PostId":"11","VoteTypeId":"2","CreationDate":"2022-02-20T00:00:00.000"}
{"Id":"17","PostId":"3","VoteTypeId":"3","CreationDate":"2022-02-27T00:00:00.000"}
```

Then the following should be the content of your `outlier_weeks` view:

| Year | WeekNumber | VoteCount |
| ---- | ---------- | --------- |
| 2022 | 0          | 1         |
| 2022 | 1          | 3         |
| 2022 | 2          | 3         |
| 2022 | 5          | 1         |
| 2022 | 6          | 1         |
| 2022 | 8          | 1         |

**Note that we strongly encourage you to use this data as a test case to ensure that you have the correct calculation!**

## Follow-up Questions

Please include instructions about your strategy and important decisions you made in the README file. You should also include answers to the following questions:

1. What kind of data quality measures would you apply to your solution in production?
2. What would need to change for the solution scale to work with a 10TB dataset with 5GB new data arriving each day?
3. Please tell us in your modified README about any assumptions you have made in your solution (below).

## Your Approach and answers to follow-up questions

_Please provide an explaination to your implementation approach and the additional questions **here**_

1. **What kind of data quality measures would you apply to your solution in production?**

   * Checking for missing values i.e Data Completeness, ensuring all required data fields are filled.
   * Verify uniform data format across datasets ensuring data consistency.
   * Validating data by eliminate duplicate records ensuring uniqueness.
   * Validate that data correctly represents the real-world values ensuring data accuracy.
2. **What would need to change for the solution scale to work with a 10TB dataset with 5GB new data arriving each day?**

   * Distributed Database can be used - Use of distributed storage solution like HDFS, Amazon S3, or cloud-based data lakes.
   * Pagination / Partitioning data - Implement horizontal partitioning or sharding to distribute data across multiple storage nodes for faster access.
   * Indexing - Optimize indexing for faster querying on large datasets.
   * Batch/Stream Processing - Implement batch processing (e.g., Apache Spark) or stream processing (e.g., Kafka) to handle the incoming data efficiently as 5GB new data arriving each day.
   * Distributed Computing - Utilize distributed computing frameworks like Apache Hadoop or Spark for parallel processing.
   * NoSQL database can be an option if scenarios demand a different situation such as unstructured data. Mongo DB or cassandra can be used as tool.
   * Compression - Apply data compression techniques to reduce storage costs and I/O overhead.
   * Data Retention Policies - Implement data retention and archiving strategies to manage historical data.
3. **Please tell us in your modified README about any assumptions you have made in your solution (below).**

   * Assumption in db and ingestion made :-
     * In the votes.jsonl file there are few rows which are having 5 columns of data and rest are having 4 columns of data, we can observe there is extra column as "***UserID***" in few rows. e.g below
       * {"Id":"24","PostId":"14","VoteTypeId":"2","CreationDate":"2017-02-28T00:00:00.000"}
       * {"Id":"25",_**"UserId":"57"**_,"PostId":"14","VoteTypeId":"5","CreationDate":"2017-02-28T00:00:00.000"}
     * votes.jsonl is already has primary key as ID i.e having not null unique values.
     * On creating table, only  ***ID***  and ***creation_date*** data is required and rest of the column can be ignored, as we have to calculate the outlier week, but I have read all the four columns and as the *sample_votes.jsonl* doesn't have the 5th column '***UserId***', so I have ignored it.
     * Taken a separate config file for main code '***config.py***' and test code as '***config_test.py***'.
     * There are certain tests like creating database and schema which doesn't require pytest/unit test cases as they are already handled in duckdb built in library.
     * Created table with primary key and then inserting value with '**DISTINCT**' keyword are exclusive but considered both because, as a data scientist sometime its difficult to keep track with the table structure and format when dealing and managing with large complex databases.
     * Inserted data into bulk into db and not as reading row by row from ***votes.jsonl*** file improves the performance for insertion, but if the ***ID*** is not unique then the bulk insert get fails and throws error and no row get inserted.
     * Display function has row parameter which gets passed in code has a default value.
   * Assumption in outlier made :-
     * Used multiple ***CTE (common table expresssion)*** to calculate and evaluate outlier. Temporary tables can be used but in the given problem its not required as we don't want to persist temporary tables for the session. Memory is freed as soon as the code gets executed and view is created on the database. CTE tables get remove automatically as the scope of execution gets complete.
     * Dropping view on every query for outlier calcuation and recreating it.
     * ***For certain 1st week of the year we get value greater than equal to 52 so resolving this important problem.***
     * ***Assumption is that 1st week is count from 0(zero)th order and week number going to 52 week. Supressing the 52th week to 53rd week.***
     * Outlier week view is not organised as it was just need to be displayed.
   * Assumption in unit test cases :-
     * ***Certain unit test cases seems to be derived from excercise folder but modifed with necessary input file scope and required changes made.***
     * Unit test cases are independent modules which can be executed independently, but with the main code outlier will only execute after atleast one ingestion. Outlier can be called multiple times after that.
4. **Code Explanation :-**

   * Ingestion :- Data is loaded directly from file using "***insert into select * from file**"* command helps to load data instantly into database, row by row will take a lot of time. Using *Distinct* keyword prevent duplicate entry.
     * Display data takes row number to display rows in votes table and default row value is set to 3. To change display row things has to be changed from code itself.
   * CTE expression used to calculate outlier week as we don't need to persist or maintain for the session. CTE get deleted automatically when scope of execution completes. CTE are faster than any temporary table as well as it help with memory optimisation also.
   * DUCKDB helps with data loading directly from file which improves a lot of performance while loading data into database specially from jsonl file, csv file, parquete file.
5. **Ambiguity :-**

   * For certain 1st week of the year we get value greater than equal to 52 so resolving with ***cases*** in sql query on creation of view.
   * Ambiguity is that 1st week is count from 0(zero)th order and week number going to 52 week. Supressing the 53rd week to 52th week. Logical error supressing in ***cases*** in sql query to pass unit test cases for the samples-votes.jsonl file.
