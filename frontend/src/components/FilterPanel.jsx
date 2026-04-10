import React, { useState } from 'react'
import { useFilterStore } from '@/stores/filterStore'
import { useAllSkills } from '@/api/hooks/useSkills'
import { X, ChevronDown, Sliders } from 'lucide-react'

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
    <div className="w-full md:w-80 bg-white border-r border-gray-200 p-6 overflow-y-auto">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-xl font-bold text-gray-900 flex items-center gap-2">
          <Sliders className="w-5 h-5" />
          Filters
        </h2>
        <button
          onClick={onClose}
          className="md:hidden p-1 hover:bg-gray-100 rounded"
          aria-label="Close filters"
        >
          <X className="w-5 h-5" />
        </button>
      </div>

      {/* Location Section */}
      <FilterSection
        title="Location"
        expanded={expandedSections.location}
        onToggle={() => toggleSection('location')}
      >
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Country
            </label>
            <input
              type="text"
              placeholder="e.g., USA"
              value={store.country || ''}
              onChange={(e) => store.setCountry(e.target.value || null)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              City
            </label>
            <input
              type="text"
              placeholder="e.g., San Francisco"
              value={store.city || ''}
              onChange={(e) => store.setCity(e.target.value || null)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
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
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Keyword
            </label>
            <input
              type="text"
              placeholder="Search jobs..."
              value={store.keyword}
              onChange={(e) => store.setKeyword(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Seniority
            </label>
            <select
              value={store.seniority || ''}
              onChange={(e) => store.setSeniority(e.target.value || null)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            >
              <option value="">All Levels</option>
              <option value="entry">Entry Level</option>
              <option value="mid">Mid Level</option>
              <option value="senior">Senior Level</option>
              <option value="lead">Lead</option>
            </select>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Category
            </label>
            <input
              type="text"
              placeholder="e.g., Software Engineering"
              value={store.category || ''}
              onChange={(e) => store.setCategory(e.target.value || null)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
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
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Match Type
            </label>
            <select
              value={store.skillMatchType}
              onChange={(e) => store.setSkillMatchType(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            >
              <option value="any">Match ANY skill</option>
              <option value="all">Match ALL skills</option>
            </select>
          </div>

          {/* Selected Skills */}
          {store.skills.length > 0 && (
            <div className="bg-blue-50 p-3 rounded-lg">
              <p className="text-xs text-gray-600 mb-2 font-medium">
                {store.skills.length} skill{store.skills.length !== 1 ? 's' : ''} selected
              </p>
              <div className="flex flex-wrap gap-2">
                {store.skills.map((skill) => (
                  <button
                    key={skill}
                    onClick={() => store.removeSkill(skill)}
                    className="inline-flex items-center gap-1 px-2 py-1 bg-blue-200 text-blue-700 rounded-full text-sm hover:bg-blue-300 transition"
                  >
                    {skill}
                    <X className="w-3 h-3" />
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Skill Selector */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
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
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
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
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Currency
            </label>
            <select
              value={store.salaryCurrency}
              onChange={(e) => store.setSalaryCurrency(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
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
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Minimum
              </label>
              <input
                type="number"
                placeholder="0"
                value={store.minSalary || ''}
                onChange={(e) => store.setMinSalary(e.target.value ? parseFloat(e.target.value) : null)}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Maximum
              </label>
              <input
                type="number"
                placeholder="1000000"
                value={store.maxSalary || ''}
                onChange={(e) => store.setMaxSalary(e.target.value ? parseFloat(e.target.value) : null)}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>
          </div>
        </div>
      </FilterSection>

      {/* Action Buttons */}
      {store.hasActiveFilters() && (
        <div className="mt-6 pt-6 border-t border-gray-200">
          <button
            onClick={() => store.reset()}
            className="w-full px-4 py-2 text-sm font-medium text-gray-700 bg-gray-100 hover:bg-gray-200 rounded-lg transition"
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
    <div className="mb-6 border-b border-gray-200 pb-6 last:border-b-0 last:mb-0 last:pb-0">
      <button
        onClick={onToggle}
        className="w-full flex items-center justify-between p-2 -m-2 hover:bg-gray-50 rounded transition"
      >
        <h3 className="font-semibold text-gray-900">{title}</h3>
        <ChevronDown
          className={`w-5 h-5 text-gray-500 transition-transform ${
            expanded ? 'transform rotate-180' : ''
          }`}
        />
      </button>
      {expanded && <div className="mt-4">{children}</div>}
    </div>
  )
}
