# MCAP Splitter (by topic)

A simple tool to split an MCAP file by topic.

## Installation

# Clone the repository:

```
git clone git@github.com:lichtblick-suite/mcap-split-by-topic.git
cd mcap-split-by-topic
```

# Install dependencies:

```
npm install
```

# Usage

To split an MCAP (or multiple) file by topic:

```
npm run split <input-file.mcap> <input-file2.mcap> <input-file3.mcap>
```

Examples:

```
npm run split my_file.mcap
npm run split my_file.mcap ../my_other_file.mcap ~/mcaps/other.mcap
```

This will create separate .mcap files for each topic in a directory named after the input file.
