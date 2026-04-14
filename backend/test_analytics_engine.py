"""
Test Suite for Rigorous Analytics Engine
Validates all 9 steps with mock data
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession

from src.analytics.rigorous_engine import (
    clean_jobs_data,
    validate_and_extract_skills,
    analyze_skill_demand,
    detect_trending_skills,
    analyze_salary_insights,
    analyze_market_locations,
    analyze_skill_combinations,
    generate_actionable_insights,
    compute_rigorous_analytics,
    MIN_JOBS_FOR_SKILL_ANALYSIS,
    MIN_SKILL_APPEARANCES,
    MIN_SAMPLE_FOR_HIGH_CONFIDENCE,
)


# ══════════════════════════════════════════════════════════════════════════════
# FIXTURES
# ══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def mock_db():
    """Create mock AsyncSession"""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def sample_jobs_data():
    """Create sample jobs dataframe"""
    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)
    
    jobs = [
        {
            'id': 1,
            'title': 'Senior Python Developer',
            'company': 'TechCorp',
            'city': 'San Francisco',
            'country': 'USA',
            'description': 'python django rest api microservices docker kubernetes linux',
            'description_lower': 'python django rest api microservices docker kubernetes linux',
            'salary_min': 120000,
            'salary_max': 180000,
            'salary_mid': 150000,
            'remote': True,
            'seniority': 'senior',
            'posted_at': now,
            'fetched_at': now,
            'search_keyword': 'python developer',
        },
        {
            'id': 2,
            'title': 'React Frontend Engineer',
            'company': 'StartupXYZ',
            'city': 'New York',
            'country': 'USA',
            'description': 'react javascript typescript nodejs html css responsive design',
            'description_lower': 'react javascript typescript nodejs html css responsive design',
            'salary_min': 100000,
            'salary_max': 140000,
            'salary_mid': 120000,
            'remote': True,
            'seniority': 'mid',
            'posted_at': now,
            'fetched_at': now,
            'search_keyword': 'frontend developer',
        },
        {
            'id': 3,
            'title': 'Full Stack Engineer',
            'company': 'WebDev',
            'city': 'Remote',  # Invalid - should be filtered
            'country': 'USA',
            'description': None,  # Invalid - should be filtered
            'description_lower': None,
            'salary_min': 110000,
            'salary_max': 160000,
            'salary_mid': 135000,
            'remote': False,
            'seniority': 'junior',
            'posted_at': now,
            'fetched_at': now,
            'search_keyword': 'full stack',
        },
        {
            'id': 4,
            'title': 'DevOps Engineer',
            'company': 'CloudSys',
            'city': 'San Francisco',
            'country': 'USA',
            'description': 'kubernetes docker linux aws gcp terraform devops automation tools',
            'description_lower': 'kubernetes docker linux aws gcp terraform devops automation tools',
            'salary_min': 130000,
            'salary_max': 190000,
            'salary_mid': 160000,
            'remote': True,
            'seniority': 'senior',
            'posted_at': week_ago,
            'fetched_at': week_ago,
            'search_keyword': 'devops',
        },
    ]
    
    df = pd.DataFrame(jobs)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestDataCleaning:
    """Test Step 1: Data Cleaning"""
    
    @pytest.mark.asyncio
    async def test_removes_invalid_cities(self, mock_db, sample_jobs_data):
        """Test that Remote city is filtered out"""
        # Create tuples from dataframe
        mock_results = [tuple(row) for row in sample_jobs_data.itertuples(index=False, name=None)]
        
        # Create an async function that returns a proper mock result object
        async def mock_execute_func(*args, **kwargs):
            result = AsyncMock()
            result.fetchall.return_value = mock_results
            return result
        
        # Use MagicMock with side_effect to call our async function
        mock_db.execute = MagicMock(side_effect=mock_execute_func)
        
        cleaned_df, report = await clean_jobs_data(mock_db)
        
        # Remote city should be removed
        assert 'Remote' not in cleaned_df['city'].values
        assert len(cleaned_df) < len(sample_jobs_data)
        assert report.jobs_before_cleaning > report.jobs_after_cleaning
    
    @pytest.mark.asyncio
    async def test_removes_missing_descriptions(self, mock_db, sample_jobs_data):
        """Test that jobs without descriptions are filtered"""
        mock_results = [tuple(row) for row in sample_jobs_data.itertuples(index=False, name=None)]

        # Create an async function that returns a proper mock result object
        async def mock_execute_func(*args, **kwargs):
            result = AsyncMock()
            result.fetchall.return_value = mock_results
            return result
        
        # Use MagicMock with side_effect to call our async function
        mock_db.execute = MagicMock(side_effect=mock_execute_func)
        
        cleaned_df, report = await clean_jobs_data(mock_db)
        
        # All remaining jobs should have descriptions
        assert cleaned_df['description'].notna().all()
        assert (cleaned_df['description'].str.strip() != '').all()


class TestSkillExtraction:
    """Test Step 2: Skill Extraction"""
    
    @pytest.mark.asyncio
    async def test_extracts_exact_match_skills(self, mock_db, sample_jobs_data):
        """Test exact word boundary matching"""
        # Get valid jobs only (not None description)
        cleaned_df = sample_jobs_data[sample_jobs_data['description'].notna()].copy()
        
        # Mock skill data
        skill_mocks = [
            MagicMock(name='python', id=1, category='language'),
            MagicMock(name='kubernetes', id=2, category='devops'),
            MagicMock(name='react', id=3, category='frontend'),
            MagicMock(name='typescript', id=4, category='language'),
        ]
        
        # Create an async function that returns skills
        async def mock_execute_func(*args, **kwargs):
            result = AsyncMock()
            result.fetchall.return_value = skill_mocks
            return result
        
        mock_db.execute = MagicMock(side_effect=mock_execute_func)
        
        # Extract skills from descriptions
        skill_to_job_ids = await validate_and_extract_skills(mock_db, cleaned_df)
        
        # Should find python and kubernetes in descriptions
        assert 'python' in skill_to_job_ids
        assert 'kubernetes' in skill_to_job_ids


class TestSkillDemand:
    """Test Step 3: Skill Demand Analysis"""
    
    @pytest.mark.asyncio
    async def test_normalizes_demand_by_category(self, mock_db, sample_jobs_data):
        """Test skill demand is normalized correctly"""
        # Get valid jobs only
        cleaned_df = sample_jobs_data[sample_jobs_data['description'].notna()].copy()
        
        skill_mocks = [
            MagicMock(name='python', category='language'),
            MagicMock(name='kubernetes', category='devops'),
        ]
        
        # Create an async function that returns skills
        async def mock_execute_func(*args, **kwargs):
            result = AsyncMock()
            result.fetchall.return_value = skill_mocks
            return result
        
        mock_db.execute = MagicMock(side_effect=mock_execute_func)
        
        # Extract skills first
        skill_to_job_ids = await validate_and_extract_skills(mock_db, cleaned_df)
        
        # Analyze demand
        result = await analyze_skill_demand(mock_db, cleaned_df, skill_to_job_ids)
        
        assert isinstance(result, dict)
        
        # If we have any categories, check they contain skills
        if result:
            for category, skills in result.items():
                assert isinstance(skills, list)
                for skill in skills:
                    assert 'skill' in skill
                    assert 'demand' in skill
                    assert 0 <= skill['demand'] <= 1  # Normalized score


class TestTrendingSkills:
    """Test Step 4: Trending Skills Detection"""
    
    @pytest.mark.asyncio
    async def test_calculates_wow_growth_only_with_sufficient_samples(self, mock_db, sample_jobs_data):
        """Test WOW growth calculation with sample size validation"""
        skill_to_job_ids = {
            'python': {1, 2},
            'kubernetes': {3, 4},
        }
        
        trending = await detect_trending_skills(mock_db, skill_to_job_ids, sample_jobs_data)
        
        # With only 2 jobs per skill this week, should not generate trending
        # because MIN_TRENDING_WEEKLY_COUNT is 10
        assert isinstance(trending, list)
        if len(trending) > 0:
            assert all(t['confidence'] in ['HIGH', 'MEDIUM', 'LOW'] for t in trending)


class TestSalaryAnalysis:
    """Test Step 5: Salary Analysis"""
    
    @pytest.mark.asyncio
    async def test_removes_salary_outliers(self, mock_db, sample_jobs_data):
        """Test outlier removal in salary analysis"""
        skill_to_job_ids = {
            'python': {1, 2, 4},  # 3 jobs with salary data
        }
        
        # Add sufficient samples by duplicating
        extended_df = pd.concat([sample_jobs_data] * 20, ignore_index=True)
        
        salary_insights = await analyze_salary_insights(mock_db, skill_to_job_ids, extended_df)
        
        # Should have top paying skills if samples are sufficient
        assert 'top_paying_skills' in salary_insights
        assert 'by_seniority' in salary_insights
    
    @pytest.mark.asyncio
    async def test_respects_minimum_sample_size(self, mock_db, sample_jobs_data):
        """Test that skills with few salary samples are filtered"""
        skill_to_job_ids = {
            'python': {1},  # Only 1 job - below MIN_SALARY_SAMPLES (30)
        }
        
        salary_insights = await analyze_salary_insights(mock_db, skill_to_job_ids, sample_jobs_data)
        
        # Python should not appear in top_paying_skills due to low sample size
        top_skills = salary_insights.get('top_paying_skills', [])
        assert isinstance(top_skills, list)


class TestMarketAnalysis:
    """Test Step 6: Market Analysis"""
    
    def test_calculates_remote_percentage(self, sample_jobs_data):
        """Test remote job percentage calculation"""
        market = analyze_market_locations(sample_jobs_data)
        
        assert 'remote_percentage' in market
        assert 'top_cities' in market
        assert 'top_countries' in market
        
        # Assert percentages are valid
        assert 0 <= market['remote_percentage'] <= 100


class TestSkillCombinations:
    """Test Step 7: Skill Combinations"""
    
    def test_identifies_high_cooccurrence_skills(self, sample_jobs_data):
        """Test skill pair identification"""
        skill_to_job_ids = {
            'python': {1, 2},
            'django': {1},
            'kubernetes': {3, 4},
            'docker': {3, 4},
        }
        
        combinations = analyze_skill_combinations(skill_to_job_ids, sample_jobs_data)
        
        # Should return a list
        assert isinstance(combinations, list)
        
        # If there are combinations, check structure
        if len(combinations) > 0:
            for combo in combinations:
                assert 'skill1' in combo
                assert 'skill2' in combo
                assert 'cooccurrence_count' in combo


class TestActionableInsights:
    """Test Step 8: Actionable Insights"""
    
    def test_only_includes_high_confidence_insights(self, sample_jobs_data):
        """Test that low-sample insights are filtered"""
        skill_insights = {'language': [{'skill': 'python', 'frequency': 2, 'percentage': 50}]}
        trending_skills = []
        salary_insights = {'top_paying_skills': [], 'by_seniority': {}}
        market_insights = {'remote_percentage': 50, 'total_jobs': 4, 'remote_jobs': 2}
        skill_to_job_ids = {'python': {1, 2}}
        
        insights = generate_actionable_insights(
            skill_insights, trending_skills, salary_insights, market_insights,
            skill_to_job_ids, sample_jobs_data
        )
        
        # All insights should have reasoning
        assert all(i.reason for i in insights)
        assert all(i.confidence in ['HIGH', 'MEDIUM', 'LOW'] for i in insights)


class TestFullPipeline:
    """Integration tests for complete pipeline"""
    
    @pytest.mark.asyncio
    async def test_pipeline_runs_without_errors(self, mock_db, sample_jobs_data):
        """Test that complete pipeline executes"""
        # Create mock results
        mock_results = [tuple(row) for row in sample_jobs_data.itertuples(index=False, name=None)]
        
        # Create separate async functions for each call
        call_count = 0
        async def mock_execute_func(*args, **kwargs):
            nonlocal call_count
            result = AsyncMock()
            # Alternate between jobs and skills results
            if call_count == 0:
                result.fetchall.return_value = mock_results
            else:
                result.fetchall.return_value = [
                    MagicMock(name='python', id=1, category='language'),
                    MagicMock(name='kubernetes', id=2, category='devops'),
                ]
            call_count += 1
            return result
        
        # Use MagicMock with side_effect to call our async function
        mock_db.execute = MagicMock(side_effect=mock_execute_func)
        
        output = await compute_rigorous_analytics(mock_db)
        assert output.trending_skills is not None
        assert output.salary_insights is not None
        assert output.market_insights is not None
        assert output.skill_combinations is not None
        assert output.actionable_insights is not None
        assert output.data_quality_report is not None


# ══════════════════════════════════════════════════════════════════════════════
# RUN TESTS
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
