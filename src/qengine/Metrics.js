function getKeywordMatchCondition(keyword) {
  return {
    $match: {
      fullText: { $regex: new RegExp(keyword, "i") },
    },
  };
}

function getHashTagMatchCondition(hashTag) {
  return {
    $match: {
      hashTags: {
        $elemMatch: { $regex: new RegExp(`^${hashTag}$`, "i") },
      },
    },
  };
}

module.exports = {
  sentiment: (params) => {
    const pipeline = [];
    if (params.keyword && params.keyword.trim()) {
      pipeline.push(getKeywordMatchCondition(params.keyword.trim()));
    }

    pipeline.push({
      $project: { sentiment: true },
    });

    return pipeline;
  },
  locations: (params) => {
    const pipeline = [];
    if (params.keyword && params.keyword.trim()) {
      pipeline.push(getKeywordMatchCondition(params.keyword.trim()));
    }

    pipeline.push({
      $match: {
        $and: [{ latitude: { $ne: 0 } }, { latitude: { $ne: 0 } }],
      },
    });

    pipeline.push({
      $project: {
        latitude: 1,
        longitude: 1,
      },
    });

    return pipeline;
  },
  tweetstrend: (params) => {
    const pipeline = [];
    if (params.keyword && params.keyword.trim()) {
      pipeline.push(getKeywordMatchCondition(params.keyword.trim()));
    }

    const unit = params.unit || "hour"; // day or hour

    pipeline.push({
      $group: {
        _id: {
          time: {
            $dateTrunc: {
              date: "$createdAtUTC",
              unit,
              binSize: 1,
            },
          },
        },
        count: { $sum: 1 },
      },
    });

    pipeline.push({
      $sort: { "_id.time": 1 },
    });

    pipeline.push({
      $project: {
        _id: false,
        time: "$_id.time",
        count: true,
      },
    });

    return pipeline;
  },
  hashtagstrend: (params) => {
    const pipeline = [];
    if (params.keyword && params.keyword.trim()) {
      pipeline.push(getHashTagMatchCondition(params.keyword.trim()));
    }

    const unit = params.unit || "hour"; // day or hour

    pipeline.push({
      $group: {
        _id: {
          time: {
            $dateTrunc: {
              date: "$createdAtUTC",
              unit,
              binSize: 1,
            },
          },
        },
        count: { $sum: 1 },
      },
    });

    pipeline.push({
      $sort: { "_id.time": 1 },
    });

    pipeline.push({
      $project: {
        _id: false,
        time: "$_id.time",
        count: true,
      },
    });

    return pipeline;
  },
  tweetsbycountry: (params) => {
    const pipeline = [];
    if (params.keyword && params.keyword.trim()) {
      pipeline.push(getKeywordMatchCondition(params.keyword.trim()));
    }

    pipeline.push({
      $match: { country: { $ne: "" } },
    });

    pipeline.push({
      $group: {
        _id: "$country",
        count: { $sum: 1 },
      },
    });

    pipeline.push({
      $project: {
        _id: false,
        country: "$_id",
        count: true,
      },
    });

    return pipeline;
  },
  hashtagsbycountry: (params) => {
    const pipeline = [];
    if (params.keyword && params.keyword.trim()) {
      pipeline.push(getHashTagMatchCondition(params.keyword.trim()));
    }

    pipeline.push({
      $match: { country: { $ne: "" } },
    });

    pipeline.push({
      $group: {
        _id: "$country",
        count: { $sum: 1 },
      },
    });

    pipeline.push({
      $project: {
        _id: false,
        country: "$_id",
        count: true,
      },
    });

    return pipeline;
  },
};
