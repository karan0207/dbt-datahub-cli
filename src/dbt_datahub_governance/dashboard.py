"""Web dashboard for dbt-datahub-governance."""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Governance Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Professional CSS Theme ---
st.markdown("""
<style>
    /* Global Font & Reset */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    html, body, [class*="css"]  {
        font-family: 'Inter', sans-serif;
    }

    /* Remove Default Streamlit Padding */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1400px;
    }

    /* --- Theme Aware Colors --- */
    
    /* Headers have default streamlit colors which adapt to theme */
    h1, h2, h3 {
        font-weight: 700;
        letter-spacing: -0.025em;
    }
    
    /* Metrics - Transparent background to blend with theme */
    div[data-testid="stMetric"] {
        background-color: rgba(128, 128, 128, 0.05);
        border: 1px solid rgba(128, 128, 128, 0.1);
        padding: 1rem;
        border-radius: 8px;
    }
    
    /* Sidebar - remove forced background */
    section[data-testid="stSidebar"] {
        border-right: 1px solid rgba(128, 128, 128, 0.1);
    }

    /* Buttons */
    div.stButton > button {
        border-radius: 6px;
        font-weight: 600;
        border: 1px solid rgba(128, 128, 128, 0.2);
    }

    /* Containers */
    div.stContainer {
        padding: 1rem 0;
    }
    
    /* Expander Styling */
    .streamlit-expanderHeader {
        background-color: rgba(128, 128, 128, 0.02);
        border-radius: 4px;
    }
    
    /* Hide Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
</style>
""", unsafe_allow_html=True)

# --- Helper Functions ---

def get_datahub_connection() -> tuple[Optional[str], Optional[str]]:
    """Get DataHub connection from environment or session state."""
    server = st.session_state.get("datahub_server") or os.environ.get("DATAHUB_GMS_URL")
    token = st.session_state.get("datahub_token") or os.environ.get("DATAHUB_GMS_TOKEN")
    return server, token


def run_validation(
    manifest_path: str,
    config_path: Optional[str] = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run governance validation and return results."""
    from dbt_datahub_governance.config import load_config
    from dbt_datahub_governance.datahub import DataHubClient, MockDataHubClient
    from dbt_datahub_governance.datahub.urn_mapper import UrnMapper
    from dbt_datahub_governance.models.governance import DatasetGovernanceStatus
    from dbt_datahub_governance.parsers import load_dbt_project
    from dbt_datahub_governance.rules import GovernanceEngine

    dbt_manifest = load_dbt_project(Path(manifest_path))
    governance_config = load_config(Path(config_path) if config_path else None)
    
    if dry_run:
        mapper = UrnMapper(
            platform=governance_config.target_platform,
            env=governance_config.environment,
        )
        mock_data = {}
        for model in dbt_manifest.models.values():
            urn = mapper.model_to_urn(model)
            mock_data[urn] = DatasetGovernanceStatus(
                urn=urn,
                exists=True,
                has_owner=True,
                has_description=bool(model.description),
                has_domain=False,
                has_tags=bool(model.tags),
                owners=["urn:li:corpuser:dry-run-owner"],
                description=model.description,
                tags=[f"urn:li:tag:{t}" for t in model.tags],
            )
        datahub_client = MockDataHubClient(mock_data=mock_data)
    else:
        server, token = get_datahub_connection()
        if not server:
            raise ValueError("DataHub server URL not configured")
        datahub_client = DataHubClient(server=server, token=token)
    
    engine = GovernanceEngine(
        config=governance_config,
        datahub_client=datahub_client,
        manifest=dbt_manifest,
    )
    report = engine.validate()
    
    return {
        "report": report,
        "manifest": dbt_manifest,
        "config": governance_config,
        "timestamp": datetime.now().isoformat(),
    }


# --- Layout Components ---

def render_sidebar():
    with st.sidebar:
        st.markdown(
            """
            <div style="padding: 1rem 0;">
                <h2 style="font-size: 1.4rem; margin-bottom: 0.2rem;">🛡️ Governance</h2>
                <div style="color: #64748b; font-size: 0.85rem;">Unified dbt & DataHub Control</div>
            </div>
            """,
            unsafe_allow_html=True
        )
        
        st.divider()
        
        with st.expander("🔌 Connection", expanded=True):
            server = st.text_input(
                "DataHub Host",
                value=os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080"),
                placeholder="http://localhost:8080"
            )
            token = st.text_input(
                "Access Token",
                type="password",
                value=os.environ.get("DATAHUB_GMS_TOKEN", ""),
                placeholder="Optional"
            )
            
            if server:
                st.session_state["datahub_server"] = server
            if token:
                st.session_state["datahub_token"] = token
                
            if st.button("Test Connection", use_container_width=True):
                try:
                    import requests
                    with st.spinner("Connecting..."):
                        response = requests.get(f"{server}/config", timeout=5)
                        if response.status_code == 200:
                            st.success("Connected")
                        else:
                            st.error(f"Failed: {response.status_code}")
                except Exception as e:
                    st.error(f"Error: {str(e)}")

        with st.expander("📂 Project", expanded=True):
            manifest_path = st.text_input(
                "Manifest Path",
                value="examples/sample_manifest.json",
            )
            config_path = st.text_input(
                "Policies Config",
                value="",
                placeholder="governance.yml"
            )
            
            st.session_state["manifest_path"] = manifest_path
            st.session_state["config_path"] = config_path if config_path else None

def render_overview():
    st.markdown("## Dashboard")
    
    # Action Bar
    with st.container():
        col1, col2 = st.columns([3, 1])
        with col1:
             st.markdown(
                """<div style="color: #64748b; padding-top: 5px;">
                Validate dbt models against organization policies defined in DataHub.
                </div>""", unsafe_allow_html=True)
        with col2:
            dry_run = st.toggle("Mock Data (Dry Run)", value=True)
            if st.button("▶ Run Validation", type="primary", use_container_width=True):
                manifest_path = st.session_state.get("manifest_path", "examples/sample_manifest.json")
                config_path = st.session_state.get("config_path")
                
                with st.status("Validation in progress..."):
                    try:
                        results = run_validation(manifest_path, config_path, dry_run)
                        st.session_state["last_results"] = results
                    except Exception as e:
                        st.error(f"Execution failed: {str(e)}")

    if "last_results" in st.session_state:
        results = st.session_state["last_results"]
        report = results["report"]
        
        st.markdown("### Compliance Overview")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Models", report.total_models_checked)
        m2.metric("Checks Passed", f"{int((report.passed / report.total_checks)*100) if report.total_checks > 0 else 0}%")
        m3.metric("Critical Issues", report.errors, delta=-report.errors if report.errors > 0 else 0, delta_color="inverse")
        m4.metric("Warnings", report.warnings)

        st.markdown("### Violation Report")
        
        errors = report.get_errors()
        warnings = report.get_warnings()
        
        if not errors and not warnings:
             st.success("✅ Excellent! Project is fully compliant.")
        
        tabs = st.tabs([f"Critical ({len(errors)})", f"Warnings ({len(warnings)})"])
        
        with tabs[0]:
            if errors:
                for err in errors:
                    st.markdown(
                        f"""
                        <div style="padding: 1rem; background: rgba(239, 68, 68, 0.1); border: 1px solid rgba(239, 68, 68, 0.2); border-radius: 8px; margin-bottom: 0.5rem; display: flex; justify-content: space-between; align-items: center;">
                            <div>
                                <strong style="font-size: 1.1rem;">{err.model_name}</strong>
                                <div style="font-size: 0.9rem; opacity: 0.8;">{err.rule_name}: {err.message}</div>
                            </div>
                            <span style="background: rgba(239, 68, 68, 0.8); color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem;">ERROR</span>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
            else:
                st.info("No critical issues found.")
                
        with tabs[1]:
            if warnings:
                for warn in warnings:
                    st.markdown(
                        f"""
                        <div style="padding: 1rem; background: rgba(245, 158, 11, 0.1); border: 1px solid rgba(245, 158, 11, 0.2); border-radius: 8px; margin-bottom: 0.5rem; display: flex; justify-content: space-between; align-items: center;">
                            <div>
                                <strong style="font-size: 1.1rem;">{warn.model_name}</strong>
                                <div style="font-size: 0.9rem; opacity: 0.8;">{warn.rule_name}: {warn.message}</div>
                            </div>
                            <span style="background: rgba(245, 158, 11, 0.8); color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem;">WARN</span>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
            else:
                st.info("No warnings found.")

    else:
        st.info("👋 Ready to run. Click 'Run Validation' to start.")


def render_model_explorer():
    st.markdown("## Asset Inspector")
    
    if "last_results" not in st.session_state:
        st.warning("Please run a validation first.")
        return
        
    results = st.session_state["last_results"]
    manifest = results["manifest"]
    report = results["report"]
    
    col1, col2 = st.columns([1, 2.5])
    
    with col1:
        st.markdown("### Select Asset")
        models = sorted(list(manifest.models.keys()))
        selected_model_id = st.selectbox(
            "Model", 
            options=models,
            format_func=lambda x: x.split('.')[-1],
            label_visibility="collapsed"
        )
        model = manifest.models[selected_model_id]
        
        st.markdown("### Properties")
        st.markdown(f"**Schema**: `{model.schema}`")
        st.markdown(f"**Materialization**: `{model.config.get('materialized', 'view')}`")
        
    with col2:
        st.markdown(f"### {model.name}")
        if model.description:
            st.markdown(f"_{model.description}_")
        else:
            st.caption("No description provided.")
            
        st.divider()
        
        model_results = report.get_results_for_model(model.unique_id)
        
        st.markdown("#### Governance Checks")
        for res in model_results:
            if res.passed:
                icon = "✅"
                bg = "rgba(16, 185, 129, 0.1)" # Green tint
                border = "rgba(16, 185, 129, 0.2)"
            else:
                icon = "⛔" if res.severity.value == "error" else "⚠️"
                bg = "rgba(239, 68, 68, 0.1)" if res.severity.value == "error" else "rgba(245, 158, 11, 0.1)"
                border = "rgba(239, 68, 68, 0.2)" if res.severity.value == "error" else "rgba(245, 158, 11, 0.2)"

            st.markdown(
                f"""
                <div style="
                    margin-bottom: 10px; 
                    padding: 12px; 
                    background-color: {bg}; 
                    border: 1px solid {border}; 
                    border-radius: 6px;
                    display: flex;
                    align-items: center;
                ">
                    <div style="font-size: 1.2rem; margin-right: 12px;">{icon}</div>
                    <div>
                        <div style="font-weight: 600;">{res.rule_name}</div>
                        <div style="font-size: 0.9rem; opacity: 0.9;">{res.message}</div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )

def render_policies():
    st.markdown("## Governance Policies")
    from dbt_datahub_governance.rules import RULE_REGISTRY
    
    for rule_name, rule_class in RULE_REGISTRY.items():
        with st.expander(f"{rule_name}", expanded=False):
            st.markdown(f"**Description**: {rule_class.description}")

def render_export():
    st.markdown("## Export Report")
    
    if "last_results" not in st.session_state:
        st.warning("Please run a validation first to generate reports.")
        return
        
    results = st.session_state["last_results"]
    report = results["report"]
    report_dict = report.to_dict()
    
    # Generate Markdown content
    md_lines = [
        "# Governance Validation Report",
        f"**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Summary",
        f"- **Total Models**: {report.total_models_checked}",
        f"- **Checks Passed**: {report.passed}",
        f"- **Errors**: {report.errors}",
        f"- **Warnings**: {report.warnings}",
        "",
        "## Critical Issues"
    ]
    
    for err in report.get_errors():
        md_lines.append(f"- **{err.model_name}** ({err.rule_name}): {err.message}")
        
    md_lines.append("")
    md_lines.append("## Warnings")
    for warn in report.get_warnings():
        md_lines.append(f"- **{warn.model_name}** ({warn.rule_name}): {warn.message}")
        
    md_content = "\n".join(md_lines)
    json_content = json.dumps(report_dict, indent=2)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### JSON Format")
        st.download_button(
            label="⬇️ Download JSON",
            data=json_content,
            file_name=f"governance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            use_container_width=True,
        )
        st.code(json_content, language="json", line_numbers=True)
        
    with col2:
        st.markdown("### Markdown Format")
        st.download_button(
            label="⬇️ Download Markdown",
            data=md_content,
            file_name=f"governance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md",
            mime="text/markdown",
            use_container_width=True,
        )
        st.code(md_content, language="markdown", line_numbers=True)

# --- Main App ---

def main():
    render_sidebar()
    
    tab1, tab2, tab3, tab4 = st.tabs(["Dashboard", "Asset Inspector", "Policies", "Export"])
    
    with tab1:
        render_overview()
    with tab2:
        render_model_explorer()
    with tab3:
        render_policies()
    with tab4:
        render_export()

if __name__ == "__main__":
    main()
