import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

labels = ["Pandas", "PySpark-1", "PySpark-2"]
times = [0.605213, 6.201678, 6.696378]

plt.figure(figsize=(8, 5))
bars = plt.bar(labels, times, color=["#4C78A8", "#F58518", "#E45756"])

plt.title("A-3 Performance Comparison")
plt.xlabel("Implementation")
plt.ylabel("Execution Time (seconds)")

for bar, value in zip(bars, times):
    plt.text(
        bar.get_x() + bar.get_width() / 2,
        value,
        f"{value:.3f}s",
        ha="center",
        va="bottom"
    )

plt.tight_layout()
plt.savefig("/root/keshe/a3_performance_compare.png", dpi=200)
print("saved to /root/keshe/a3_performance_compare.png")
