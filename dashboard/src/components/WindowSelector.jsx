const OPTIONS = [
  { label: "24h", value: "1d" },
  { label: "7d", value: "7d" },
  { label: "30d", value: "30d" },
];

export default function WindowSelector({ value, onChange }) {
  return (
    <div className="window-selector">
      {OPTIONS.map((opt) => (
        <button
          key={opt.value}
          type="button"
          className={opt.value === value ? "active" : ""}
          onClick={() => onChange(opt.value)}
        >
          {opt.label}
        </button>
      ))}
    </div>
  );
}
