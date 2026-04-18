from api.app.services.provider_chart_service import build_provider_handoff_summary


def test_build_provider_handoff_summary_surfaces_clinical_story() -> None:
    chart = {
        "profile": {"age": 68, "gender": "male"},
        "summary": {
            "readmission_risk": "high",
            "visits_12m": 9,
            "active_medication_count": 12,
        },
        "emergency_snapshot": {
            "active_allergy_count": 2,
            "high_alert_count": 1,
            "penicillin_allergy_count": 1,
        },
        "acute_care_summary": {
            "acute_visits_90d": 4,
            "admissions_365d": 1,
            "last_acute_visit": "2026-04-10T04:27:16",
            "last_acute_setting": "Emergency",
            "last_acute_provider": "Mercy General",
        },
        "active_problems": [
            {"problem": "Diabetes mellitus"},
            {"problem": "Chronic kidney disease"},
            {"problem": "Heart failure"},
        ],
        "care_gaps": [
            {"care_gap": "Frequent acute care use", "suggested_action": "Arrange close PCP follow-up."},
            {"care_gap": "Polypharmacy review", "suggested_action": "Review the active medication list with the patient."},
        ],
        "medication_safety_alerts": [
            {"alert": "Penicillin allergy conflict", "suggested_action": "Avoid penicillin-class therapy and verify alternatives before ordering."}
        ],
        "abnormal_labs": [
            {"lab_name": "Hemoglobin A1c", "result": "8.6 %", "flag": "High", "suggested_follow_up": "Review diabetic regimen and follow-up interval."}
        ],
        "recent_encounters": [
            {"start_date": "2026-04-10T04:27:16", "encounter_type": "Emergency room admission", "provider": "Mercy General"}
        ],
    }

    summary = build_provider_handoff_summary(chart)

    assert "68-year-old male" in summary["snapshot"]
    assert "high readmission risk" in summary["snapshot"]
    assert "Diabetes mellitus" in summary["problem_summary"]
    assert "penicillin allergy on record" in summary["safety_summary"]
    assert summary["recommended_actions"][0] == "Arrange close PCP follow-up."
    assert len(summary["rows"]) == 5


def test_build_provider_handoff_summary_has_safe_fallbacks() -> None:
    summary = build_provider_handoff_summary(
        {
            "profile": {},
            "summary": {},
            "emergency_snapshot": {},
            "acute_care_summary": {},
            "active_problems": [],
            "care_gaps": [],
            "medication_safety_alerts": [],
            "abnormal_labs": [],
            "recent_encounters": [],
        }
    )

    assert summary["snapshot"].startswith("Patient with low readmission risk")
    assert "No high-signal active problem list" in summary["problem_summary"]
    assert summary["recommended_actions"] == ["Continue routine provider review with the current chart context."]
