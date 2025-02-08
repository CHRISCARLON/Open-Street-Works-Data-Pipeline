# Reminder of what needs to be done

Model for impact scores

Completing v0.1.1

Impact scoring is created by combining several factors:

Street Manager Data
1. Base Impact Score (from work category):
- Major works: 5 points
- Immediate works: 3 points
- Standard/HS2: 2 points
- Minor works: 1 point

2. Additional Impact Factors:
- TTRO (Temporary Traffic Regulation Order) required: +0.5
- Traffic sensitive street: +0.5
- Traffic Management Impact:
  - High impact (road closure, contra flow etc.): +2.0
  - Medium impact (give and take, stop/go): +1.0
  - Low impact (some carriageway use): +0.5
  - No impact: 0
  - Unknown: +0.5

UPRNs
3. UPRN (Property) Density Impact:
Adds 0.3-1.5 points based on number of properties affected:
- ≤5 properties: +0.3
- ≤10: +0.4
- ≤25: +0.5
- ≤50: +0.7
- ≤100: +0.9
- ≤200: +1.1
- ≤500: +1.3
- >500: +1.5

The model:
1. Combines data from both in-progress and completed works
2. Joins with UPRN counts per street (using USRNs for the join)
3. Calculates individual impact scores
4. Groups by street (USRN) to get total impact
5. Includes street metadata and geometry

Final output gives each street's total impact score based on
- works activity
- traffic sensitivity
- property density
- traffic management measures in place.
