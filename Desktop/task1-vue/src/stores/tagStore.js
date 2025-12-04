import { defineStore } from 'pinia'

export const useTagStore = defineStore('tag', {
  state: () => ({
    tagName: 'Ali Tag',
    createdDate: '2025-10-15 17:44:32',
    accounts: ['Ali yaseen'],
    units: [
      'Test Car', '1702 كادي', 'بوكسر - 2512', 'Sprinter - 1249', '518 مرسيدس',
      'كادي Aljaber', 'ايڤيكو ديلي', 'اتيكو 1568', 'متسوبشي هنتر 5160',
      '0054 - سيات', 'اسوزو'
    ],
    drivers: ['Firas Shaarawi', 'Ashraf Skafi', 'Ahmad', 'Ahmad', 'Driver1', 'Tawfiq1'],
    areas: ['Test###', 'وكالة هونداي', 'Test Test', 'Roa Test', 'New Test']
  }),
  actions: {
    updateTagName(newName) {
      this.tagName = newName
    }
  }
})
