/*
 *  Copyright 2018 Ververica GmbH
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.ververica.streamingledger.sdk.common.union;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Arrays;

/**
 * A serializer snapshot for tagged elements.
 */
public class UnionSerializerSnapshot extends CompositeTypeSerializerSnapshot<TaggedElement, UnionSerializer> {

    private static final int VERSION = 1;

    @SuppressWarnings("unused")
    public UnionSerializerSnapshot() {
        super(UnionSerializer.class);
    }

    @SuppressWarnings("WeakerAccess")
    public UnionSerializerSnapshot(UnionSerializer serializer) {
        super(serializer);
    }

    @Override
    protected int getCurrentOuterSnapshotVersion() {
        return VERSION;
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(UnionSerializer outerSerializer) {
        return outerSerializer.getUnderlyingSerializer();
    }

    @Override
    protected UnionSerializer createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
        return new UnionSerializer(Arrays.asList(nestedSerializers));
    }
}
