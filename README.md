# Easy l-diversity implementation

This implementation of the l-diversity algorithm is done as an example in Python.
The two example datasets provided in this repository are the UCI census dataset and the example dataset from the reference paper from Figure 1 on page 3.

## Assumptions

* There is only one table in the database file.
* All columns are either quasi-identifers (which will be anonymized) or sensitive information.
* The types of the columns are not changed from the input to the output table.
* The sensitive information are the rightmost columns of the table.
* All quasi-identifying columns are to be either entirely eliminated or anonymized (i.e. There is no guarantee that these columns will maintain their initial values).
* Diversification is only done if needed.

## Reference Paper

*l-diversity: Privacy Beyond k-Anonymity* by Machanavajjhala et al. (2007)
