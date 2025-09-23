/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        'sans': ['Inter', 'system-ui', 'sans-serif'],
      },
      colors: {
        bg: '#FFFFFF',
        'bg-subtle': '#F7FAFF',
        text: '#0B1220',
        muted: '#5A6B85',
        primary: {
          DEFAULT: '#1463FF',
          50: '#F0F6FF',
          100: '#E7F0FF',
          200: '#D4E3FF',
          300: '#B8CEFF',
          400: '#94B1FF',
          500: '#6B8CFF',
          600: '#1463FF',
          700: '#0F4FDB',
          800: '#0A3BA8',
          900: '#082E85',
        },
        accent: '#00B1FF',
        success: '#1DB954',
        warning: '#FFB020',
        danger: '#E64545',
      },
      boxShadow: {
        'card': '0 6px 20px rgba(0,0,0,.06)',
        'card-hover': '0 8px 25px rgba(0,0,0,.12)',
      },
      borderRadius: {
        'card': '1rem',
        'input': '0.75rem',
      },
      spacing: {
        '18': '4.5rem',
        '88': '22rem',
      },
      animation: {
        'fade-in': 'fadeIn 0.2s ease-out',
        'slide-up': 'slideUp 0.2s ease-out',
        'pulse-soft': 'pulseSoft 2s ease-in-out infinite',
      },
      keyframes: {
        fadeIn: {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
        slideUp: {
          '0%': { opacity: '0', transform: 'translateY(10px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
        pulseSoft: {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '0.7' },
        },
      },
    },
  },
  plugins: [
    require('@tailwindcss/forms'),
  ],
}