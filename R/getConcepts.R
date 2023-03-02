#' Get concepts from different fraily Indices
#'
#' @param index character vector, one of "efi", "vafi", "efragicap", "hfrs"
#'
#' @return a dataframe of FI categories and related concepts OR ICD codes
#' @export
getConcepts <- function(index) {
  if (!(index %in% c("efi", "efragicap", "vafi"))) {
    stop("oops! frailty index not found.")
  }

  if (index == "efi") {
    tribble(
      ~category, ~concept_id,
      "Activity limitation", 4058154,
      "Anaemia & haematinic deficiency", 439777,
      "Arthritis", 4291025,
      "Atrial fibrillation", 313217,
      "Cerebrovascular disease", 381316,
      "Cerebrovascular disease", 373503,
      "Chronic kidney disease", 46271022,
      "Diabetes", 201826,
      "Dizziness", 4012520,
      "Dyspnoea", 312437,
      "Falls", 436583,
      "Foot problems", 4169905,
      "Foot problems", 4182187,
      "Fragility fracture", 44791986,
      "Hearing impairment", 4038030,
      "Hearing impairment", 4246497,
      "Heart failure", 316139,
      "Heart valve disease", 4281749,
      "Housebound", 4052962,
      "Hypertension", 316866,
      "Hypotension/syncope", 443240,
      "Hypotension/syncope", 4151718,
      "Hypotension/syncope", 4037508,
      "Ischaemic heart disease", 317576,
      "Memory & cognitive problems", 443432,
      "Mobility & transfer problems", 4306934,
      "Mobility & transfer problems", 4012645,
      "Mobility & transfer problems", 4052477,
      "Mobility & transfer problems", 4052468,
      "Osteoporosis", 80502,
      "Parkinsonism & tremor", 381270,
      "Peptic ulcer", 4027663,
      "Peripheral vascular disease", 321052,
      "Requirement for care", 4080054,
      "Requirement for care", 4052331,
      "Requirement for care", 44791364,
      "Requirement for care", 4081589,
      "Requirement for care", 4080053,
      "Respiratory disease", 320136,
      "Skin ulcer", 135333,
      "Skin ulcer", 4080500,
      "Sleep disturbance", 4158489,
      "Sleep disturbance", 436962,
      "Sleep disturbance", 4156060,
      "Social vulnerability", 4309238,
      "Social vulnerability", 4022661,
      "Social vulnerability", 4143188,
      "Social vulnerability", 4053087,
      "Social vulnerability", 36716273,
      "Social vulnerability", 4019835,
      "Social vulnerability", 4307853,
      "Social vulnerability", 4307117,
      "Social vulnerability", 44791055,
      "Social vulnerability", 4218604,
      "Thyroid disease", 141253,
      "Urinary incontinence", 197672,
      "Urinary system disease", 4145656,
      "Visual impairment", 44790784,
      "Visual impairment", 4102251,
      "Visual impairment", 44791072,
      "Visual impairment", 4016895,
      "Weight loss & anorexia", 4275273,
      "Weight loss & anorexia", 40491502
    )
  } else if (index == "efragicap") {

   return(get("efragicap_concepts"))

  } else if (index == "vafi") {

      return(get("vafi_concepts"))
  }
}

