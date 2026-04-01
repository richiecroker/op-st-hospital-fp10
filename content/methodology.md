The data we use comes from the [Hospital Prescribing Dispensed in the Community](https://opendata.nhsbsa.net/dataset/hospital-prescribing-dispensed-in-the-community) dataset supplied by the NHS Business Services Authority.

We then process the data in the following ways:

- we infer the BNF chapter from the data's BNF code.  You can find out more about how this is structured in our [blog on the subject](https://www.bennett.ox.ac.uk/blog/2017/04/prescribing-data-bnf-codes/).
- We normalise BNF codes and names to the most recently available version.  From time to time [BNF codes change](https://www.nhsbsa.nhs.uk/bnf-version-changes-january-2026), and so we map any obsolete codes, to get a single unbroken dataset for each drug presentation
- Controlled Drugs category comes from the [dictionary of medicines and devices (dm+d)](https://www.nhsbsa.nhs.uk/pharmacies-gp-practices-and-appliance-contractors/nhs-dictionary-medicines-and-devices-dmd), which is  also produced by the NHS Business Services Authority.
The estimator calculates the changes in the following way:

####Assumptions and limitations

Prescribing data is only used for NHS organisations who are included in the Hospital Prescribing Dispensed in the Community dataset.  These are usually NHS Trusts.  Other prescribing undertaken in primary care, is not included.
