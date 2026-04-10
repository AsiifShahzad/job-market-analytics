import React from 'react'
import { useFilterStore } from '@/stores/filterStore.js'
import { X } from 'lucide-react'

export function Sidebar({ onClose }) {
  const {
    filters,
    setFilters,
    availableCities,
    availableCountries,
    availableSeniorities,
    availableCategories,
  } = useFilterStore()

  const handleCityChange = (city) => {
    const newCities = filters.cities.includes(city)
      ? filters.cities.filter((c) => c !== city)
      : [...filters.cities, city]
    setFilters({ cities: newCities })
  }

  const handleCountryChange = (country) => {
    const newCountries = filters.countries.includes(country)
      ? filters.countries.filter((c) => c !== country)
      : [...filters.countries, country]
    setFilters({ countries: newCountries })
  }

  const handleSeniorityChange = (seniority) => {
    const newSeniorities = filters.seniorities.includes(seniority)
      ? filters.seniorities.filter((s) => s !== seniority)
      : [...filters.seniorities, seniority]
    setFilters({ seniorities: newSeniorities })
  }

  const handleCategoryChange = (category) => {
    const newCategories = filters.categories.includes(category)
      ? filters.categories.filter((c) => c !== category)
      : [...filters.categories, category]
    setFilters({ categories: newCategories })
  }

  return (
    <div className="bg-white p-6 space-y-6">
      {/* Header with Close Button */}
      <div className="flex items-center justify-between">
        <h3 className="font-semibold text-lg text-gray-900">Filters</h3>
        {onClose && (
          <button
            onClick={onClose}
            className="md:hidden text-gray-500 hover:text-gray-700"
          >
            <X className="w-6 h-6" />
          </button>
        )}
      </div>

      {/* Cities Filter */}
      <div>
        <h4 className="font-semibold text-gray-800 mb-3">Cities</h4>
        <div className="space-y-2 max-h-48 overflow-y-auto">
          {availableCities.map((city) => (
            <label key={city} className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={filters.cities.includes(city)}
                onChange={() => handleCityChange(city)}
                className="w-4 h-4 text-purple-600 border-gray-300 rounded cursor-pointer"
              />
              <span className="text-gray-700">{city}</span>
            </label>
          ))}
        </div>
      </div>

      {/* Countries Filter */}
      <div>
        <h4 className="font-semibold text-gray-800 mb-3">Countries</h4>
        <div className="space-y-2 max-h-48 overflow-y-auto">
          {availableCountries.map((country) => (
            <label key={country} className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={filters.countries.includes(country)}
                onChange={() => handleCountryChange(country)}
                className="w-4 h-4 text-purple-600 border-gray-300 rounded cursor-pointer"
              />
              <span className="text-gray-700">{country}</span>
            </label>
          ))}
        </div>
      </div>

      {/* Seniority Filter */}
      <div>
        <h4 className="font-semibold text-gray-800 mb-3">Seniority Level</h4>
        <div className="space-y-2">
          {availableSeniorities.map((seniority) => (
            <label key={seniority} className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={filters.seniorities.includes(seniority)}
                onChange={() => handleSeniorityChange(seniority)}
                className="w-4 h-4 text-purple-600 border-gray-300 rounded cursor-pointer"
              />
              <span className="text-gray-700">{seniority}</span>
            </label>
          ))}
        </div>
      </div>

      {/* Category Filter */}
      <div>
        <h4 className="font-semibold text-gray-800 mb-3">Job Category</h4>
        <div className="space-y-2 max-h-48 overflow-y-auto">
          {availableCategories.map((category) => (
            <label key={category} className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={filters.categories.includes(category)}
                onChange={() => handleCategoryChange(category)}
                className="w-4 h-4 text-purple-600 border-gray-300 rounded cursor-pointer"
              />
              <span className="text-gray-700">{category}</span>
            </label>
          ))}
        </div>
      </div>

      {/* Clear Filters */}
      <button
        onClick={() =>
          setFilters({
            cities: [],
            countries: [],
            seniorities: [],
            categories: [],
          })
        }
        className="w-full px-4 py-2 text-gray-700 border border-gray-300 rounded-lg hover:bg-gray-50 transition"
      >
        Clear Filters
      </button>
    </div>
  )
}
