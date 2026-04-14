import React, { useState } from 'react'
import { useFilterStore } from '@/stores/filterStore'
import { useAllSkills } from '@/api/hooks/useSkills'
import { FaXmark, FaChevronDown, FaSliders } from 'react-icons/fa6'

/**
 * FilterPanel - Comprehensive filtering UI for jobs
 * Supports location, job, skill, and salary filters
 */
export const FilterPanel = ({ onClose, isOpen = true }) => {
  const store = useFilterStore()
  const { data: allSkillsData } = useAllSkills()
  const [expandedSections, setExpandedSections] = useState({
    location: true,
    job: true,
    skills: true,
    salary: true,
  })

  const allSkills = allSkillsData?.skills || []

  const toggleSection = (section) => {
    setExpandedSections((prev) => ({
      ...prev,
      [section]: !prev[section],
    }))
  }

  if (!isOpen) return null

  return (
    <div className="w-full md:w-80 bg-slate-800 border-r border-slate-700 p-6 overflow-y-auto">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-xl font-bold text-white flex items-center gap-2">
          <FaSliders className="w-5 h-5" />
          Filters
        </h2>
        <button
          onClick={onClose}
          className="md:hidden p-1 hover:bg-slate-700 rounded"
          aria-label="Close filters"
        >
          <FaXmark className="w-5 h-5 text-white" />
        </button>
      </div>

      {/* Quality Badge */}
      <div className="mb-4 p-3 bg-green-900 bg-opacity-30 border border-green-700 rounded-lg">
        <p className="text-xs font-medium text-green-300">
          ✓ All jobs verified for authenticity and quality
        </p>
      </div>

      {/* Location Section */}
      <FilterSection
        title="Location"
        expanded={expandedSections.location}
        onToggle={() => toggleSection('location')}
      >
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-2">
              Country
            </label>
            <input
              type="text"
              placeholder="e.g., USA"
              value={store.country || ''}
              onChange={(e) => store.setCountry(e.target.value || null)}
              className="w-full px-3 py-2 border border-slate-600 bg-slate-700 text-white rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-2">
              City
            </label>
            <input
              type="text"
              placeholder="e.g., San Francisco"
              value={store.city || ''}
              onChange={(e) => store.setCity(e.target.value || null)}
              className="w-full px-3 py-2 border border-slate-600 bg-slate-700 text-white rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>
        </div>
      </FilterSection>

      {/* Job Details Section */}
      <FilterSection
        title="Job Details"
        expanded={expandedSections.job}
        onToggle={() => toggleSection('job')}
      >
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-2">
              Keyword
            </label>
            <input
              type="text"
              placeholder="Search jobs..."
              value={store.keyword}
              onChange={(e) => store.setKeyword(e.target.value)}
              className="w-full px-3 py-2 border border-slate-600 bg-slate-700 text-white rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-slate-300 mb-2">
              Seniority Level
            </label>
            <select
              value={store.seniority || ''}
              onChange={(e) => store.setSeniority(e.target.value || null)}
              className="w-full px-3 py-2 border border-slate-600 bg-slate-700 text-white rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            >
              <option value="">All Levels</option>
              <option value="junior">Junior / Entry Level</option>
              <option value="mid">Mid Level</option>
              <option value="senior">Senior Level</option>
              <option value="lead">Lead / Principal</option>
              <option value="unspecified">Unspecified</option>
            </select>
          </div>

          <div>
            <label className="block text-sm font-medium text-slate-300 mb-2">
              Category
            </label>
            <input
              type="text"
              placeholder="e.g., Software Engineering"
              value={store.category || ''}
              onChange={(e) => store.setCategory(e.target.value || null)}
              className="w-full px-3 py-2 border border-slate-600 bg-slate-700 text-white rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>
        </div>
      </FilterSection>

      {/* Skills Section */}
      <FilterSection
        title="Required Skills"
        expanded={expandedSections.skills}
        onToggle={() => toggleSection('skills')}
      >
        <div className="space-y-3">
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-2">
              Match Type
            </label>
            <select
              value={store.skillMatchType}
              onChange={(e) => store.setSkillMatchType(e.target.value)}
              className="w-full px-3 py-2 border border-slate-600 bg-slate-700 text-white rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            >
              <option value="any">Match ANY skill</option>
              <option value="all">Match ALL skills</option>
            </select>
          </div>

          {/* Selected Skills */}
          {store.skills.length > 0 && (
            <div className="bg-slate-700 p-3 rounded-lg">
              <p className="text-xs text-slate-300 mb-2 font-medium">
                {store.skills.length} skill{store.skills.length !== 1 ? 's' : ''} selected
              </p>
              <div className="flex flex-wrap gap-2">
                {store.skills.map((skill) => (
                  <button
                    key={skill}
                    onClick={() => store.removeSkill(skill)}
                    className="inline-flex items-center gap-1 px-2 py-1 bg-blue-900 text-blue-300 rounded-full text-sm hover:bg-blue-800 transition"
                  >
                    {skill}
                    <FaXmark className="w-3 h-3" />
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Skill Selector */}
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-2">
              Add Skills
            </label>
            <div className="relative">
              <select
                defaultValue=""
                onChange={(e) => {
                  if (e.target.value) {
                    store.addSkill(e.target.value)
                    e.target.value = ''
                  }
                }}
                className="w-full px-3 py-2 border border-slate-600 bg-slate-700 text-white rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <option value="">Select a skill...</option>
                {allSkills.map((skill) => (
                  <option
                    key={skill.name}
                    value={skill.name}
                    disabled={store.skills.includes(skill.name)}
                  >
                    {skill.name} ({skill.frequency})
                  </option>
                ))}
              </select>
            </div>
          </div>
        </div>
      </FilterSection>

      {/* Salary Section */}
      <FilterSection
        title="Salary Range"
        expanded={expandedSections.salary}
        onToggle={() => toggleSection('salary')}
      >
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-2">
              Currency
            </label>
            <select
              value={store.salaryCurrency}
              onChange={(e) => store.setSalaryCurrency(e.target.value)}
              className="w-full px-3 py-2 border border-slate-600 bg-slate-700 text-white rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            >
              <option value="USD">USD</option>
              <option value="EUR">EUR</option>
              <option value="GBP">GBP</option>
              <option value="CAD">CAD</option>
              <option value="AUD">AUD</option>
            </select>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-2">
                Minimum
              </label>
              <input
                type="number"
                placeholder="0"
                value={store.minSalary || ''}
                onChange={(e) => store.setMinSalary(e.target.value ? parseFloat(e.target.value) : null)}
                className="w-full px-3 py-2 border border-slate-600 bg-slate-700 text-white rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-2">
                Maximum
              </label>
              <input
                type="number"
                placeholder="1000000"
                value={store.maxSalary || ''}
                onChange={(e) => store.setMaxSalary(e.target.value ? parseFloat(e.target.value) : null)}
                className="w-full px-3 py-2 border border-slate-600 bg-slate-700 text-white rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>
          </div>
        </div>
      </FilterSection>

      {/* Action Buttons */}
      {store.hasActiveFilters() && (
        <div className="mt-6 pt-6 border-t border-slate-700">
          <button
            onClick={() => store.reset()}
            className="w-full px-4 py-2 text-sm font-medium text-slate-300 bg-slate-700 hover:bg-slate-600 rounded-lg transition"
          >
            Clear All Filters
          </button>
        </div>
      )}
    </div>
  )
}

/**
 * FilterSection - Collapsible filter section component
 */
function FilterSection({ title, expanded, onToggle, children }) {
  return (
    <div className="mb-6 border-b border-slate-700 pb-6 last:border-b-0 last:mb-0 last:pb-0">
      <button
        onClick={onToggle}
        className="w-full flex items-center justify-between p-2 -m-2 hover:bg-slate-700 hover:bg-opacity-30 rounded transition"
      >
        <h3 className="font-semibold text-white">{title}</h3>
        <FaChevronDown
          className={`w-5 h-5 text-slate-400 transition-transform ${
            expanded ? 'transform rotate-180' : ''
          }`}
        />
      </button>
      {expanded && <div className="mt-4">{children}</div>}
    </div>
  )
}
