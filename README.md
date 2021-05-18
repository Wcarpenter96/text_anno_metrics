# Text Annotation Metrics Script

## Overview

Given a workers's **annotations** (a label applied to a piece of text) and two review methods of those annotations, calculate the workers's performance including:
* annotation precision
* annotation recall
* annotation overlap 
* Fleiss Kappa

## Definitions

* Annotation:  a label applied to a span
* to_be_evaluated (tbe):  the json for a chunk with annotations that we want to evaluate or get a quality measure for
* gold:  the json for a chunk with annotations that are assumed to be correct;  the standard against which to_be_evaluated chunks are measured
* True positive:  An annotation that is in to_be_evaluated and gold
* False positive:  An annotation that in to_be_evaluated but is not in gold
* False negative:  An annotation that is in gold but is not in to_be_evaluated
* Precision :=  true positives / (true positives + false positives)
* Recall := true positives / (true positives + false negatives)
* source:  a json for the pre-annotated input provided by the client
* Correction true positive:  
    * Type 1 (Correct Additions): an annotation that 
        * is not in source
        * is in to_be_annotated
        * is in gold
    * Type 2 (Correct Removals): an annotation that 
        * is in source
        * is not in to_be_annotated
        * is not in gold
* Correction false positive:
    * Type 1 (Incorrect Additions): an annotation that 
        * is not in source
        * is in to_be_annotated
        * is not in gold
    * Type 2 (Incorrect Removals): an annotation that 
        * is in source
        * is not in to_be_annotated
        * is in gold
* Correction false negatives:
    * Type 1 (Failed to add): an annotation that 
        * is not in source
        * is not in to_be_annotated
        * is in gold
    * Type 2 (Failed to remove): an annotation that 
        * is in source
        * is in to_be_annotated
        * is not in gold

## Functions

### get_disagreement_resolution()
_Comparing the annotators labeling with the disagreement resolution reviewer's labeling_
#### Helper functions:
* get_annotator_vs_review()
    * get_pr() _Get precision and recall_
        * get_totals()
        * get_precision()
        * get_recall()
    * get_correction_pr() _Get correction precision and recall_
        * get_correction_totals()
        * get_precision()
        * get_recall()
    * compare_spans() _Compare the overlap of the annotators' spans_
        * get_overlap()
        * is_partial_overlap()
        * is_superset()
        * is_subset()

### get_full_review()
_Comparing the annotators labeling with the full reviewer's labeling_
#### Helper functions:
* Same helper functions as get_disagreement_resolution()

### get_annotator_vs_annotator()
_Comparing one annotators labeling to another_
#### Helper functions:
* get_fleiss_k() _Using the formula's from this article https://www.real-statistics.com/reliability/interrater-reliability/fleiss-kappa/_



 
