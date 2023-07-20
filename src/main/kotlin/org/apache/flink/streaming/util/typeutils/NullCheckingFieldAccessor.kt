// Based on https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/util/typeutils/FieldAccessor.java
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.util.typeutils

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType.InvalidFieldReferenceException
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase
import org.apache.flink.api.java.typeutils.runtime.FieldSerializer
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase
import org.apache.flink.util.Preconditions
import scala.Product
import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.lang.reflect.Field

abstract class NullCheckingFieldAccessor<T, F> : FieldAccessor<T, F>() {
    var internalFieldType: TypeInformation<*>? = null
    override fun getFieldType(): TypeInformation<F>? {
        @Suppress("UNCHECKED_CAST")
        return internalFieldType as TypeInformation<F>?
    }

    abstract override operator fun get(record: T): F?
    abstract override operator fun set(record: T, fieldValue: F): T

    companion object {
        private const val serialVersionUID = 2L
    }
}

internal class NullCheckingRecursiveProductFieldAccessor<T, R, F>(
    pos: Int,
    typeInfo: TypeInformation<T>,
    innerAccessor: NullCheckingFieldAccessor<R, F>,
    config: ExecutionConfig?
) :
    NullCheckingFieldAccessor<T, F>() {
    private var pos = 0
    private var serializer: TupleSerializerBase<T>? = null
    private val fields: Array<Any?>
    private var length = 0
    private var innerAccessor: NullCheckingFieldAccessor<R, F>? = null
    override fun get(record: T): F? {
        @Suppress("UNCHECKED_CAST")
        return innerAccessor!!.get((record as Product).productElement(pos) as R)
    }

    override fun set(record: T, fieldValue: F): T {
        val prod = record as Product
        for (i in 0 until length) {
            fields[i] = prod.productElement(i)
        }
        @Suppress("UNCHECKED_CAST")
        fields[pos] = innerAccessor!!.set(fields[pos] as R, fieldValue)
        return serializer!!.createInstance(fields)
    }

    companion object {
        private const val serialVersionUID = 1L
    }

    init {
        val arity = (typeInfo as TupleTypeInfoBase<*>).arity
        if (pos in 0 until arity) {
            Preconditions.checkNotNull(typeInfo, "typeInfo must not be null.")
            Preconditions.checkNotNull(innerAccessor, "innerAccessor must not be null.")
            this.pos = pos
            serializer = typeInfo.createSerializer(config) as TupleSerializerBase<T>
            length = serializer!!.arity
            fields = arrayOfNulls(length)
            this.innerAccessor = innerAccessor
            super.internalFieldType = innerAccessor.getFieldType()
        } else {
            throw InvalidFieldReferenceException(
                "Tried to select " + Integer.valueOf(pos)
                    .toString() + ". field on \"" + typeInfo.toString() + "\", which is an invalid index."
            )
        }
    }
}

internal class NullCheckingSimpleProductFieldAccessor<T, F>(
    pos: Int,
    typeInfo: TypeInformation<T>,
    config: ExecutionConfig?
) :
    NullCheckingFieldAccessor<T, F>() {
    private var pos = 0
    private var serializer: TupleSerializerBase<T>? = null
    private val fields: Array<Any?>
    private var length = 0
    override fun get(record: T): F {
        val prod = record as Product
        @Suppress("UNCHECKED_CAST")
        return prod.productElement(pos) as F
    }

    override fun set(record: T, fieldValue: F): T {
        val prod = record as Product
        for (i in 0 until length) {
            fields[i] = prod.productElement(i)
        }
        fields[pos] = fieldValue
        return serializer!!.createInstance(fields)
    }

    companion object {
        private const val serialVersionUID = 1L
    }

    init {
        Preconditions.checkNotNull(typeInfo, "typeInfo must not be null.")
        val arity = (typeInfo as TupleTypeInfoBase<*>).arity
        if (pos in 0 until arity) {
            this.pos = pos
            super.internalFieldType = typeInfo.getTypeAt<TupleTypeInfo<*>>(pos)
            serializer = typeInfo.createSerializer(config) as TupleSerializerBase<T>
            length = serializer!!.arity
            fields = arrayOfNulls(length)
        } else {
            throw InvalidFieldReferenceException(
                "Tried to select " + Integer.valueOf(pos)
                    .toString() + ". field on \"" + typeInfo.toString() + "\", which is an invalid index."
            )
        }
    }
}

internal class NullCheckingPojoFieldAccessor<T, R, F>(
    field: Field,
    private val innerAccessor: NullCheckingFieldAccessor<R, F>
) :
    NullCheckingFieldAccessor<T, F>() {
    @Transient
    private var field: Field
    override fun get(record: T): F? {
        return try {
            // TODO: This null-check prevents all null issues in the current tests. Maybe we will need to add similar
            //  code to other FieldAccessors in the future.
            if (record == null) {
                return null
            }
            @Suppress("UNCHECKED_CAST")
            val inner: R = field.get(record) as R
            innerAccessor.get(inner)
        } catch (var3: IllegalAccessException) {
            throw RuntimeException(
                "This should not happen since we call setAccessible(true) in readObject. " +
                    "fields: $field obj: $record"
            )
        }
    }

    override fun set(record: T, fieldValue: F): T {
        return try {
            @Suppress("UNCHECKED_CAST")
            val inner: R = field.get(record) as R
            field[record] = innerAccessor.set(inner, fieldValue)
            record
        } catch (var4: IllegalAccessException) {
            throw RuntimeException(
                "This should not happen since we call setAccessible(true) in readObject. " +
                    "fields: $field obj: $record"
            )
        }
    }

    @Throws(IOException::class, ClassNotFoundException::class)
    private fun writeObject(out: ObjectOutputStream) {
        out.defaultWriteObject()
        FieldSerializer.serializeField(field, out)
    }

    @Throws(IOException::class, ClassNotFoundException::class)
    private fun readObject(`in`: ObjectInputStream) {
        `in`.defaultReadObject()
        field = FieldSerializer.deserializeField(`in`)
    }

    companion object {
        private const val serialVersionUID = 1L
    }

    init {
        Preconditions.checkNotNull(field, "field must not be null.")
        Preconditions.checkNotNull(innerAccessor, "innerAccessor must not be null.")
        this.field = field
        super.internalFieldType = innerAccessor.internalFieldType
    }
}

internal class NullCheckingRecursiveTupleFieldAccessor<T : Tuple?, R, F>(
    pos: Int,
    innerAccessor: NullCheckingFieldAccessor<R, F>,
    typeInfo: TypeInformation<T>
) :
    NullCheckingFieldAccessor<T, F>() {
    private var pos = 0
    private var innerAccessor: NullCheckingFieldAccessor<R, F>? = null
    override fun get(record: T): F? {
        val inner: R = record!!.getField(pos)
        return innerAccessor!![inner]
    }

    override fun set(record: T, fieldValue: F): T {
        val inner: R = record!!.getField(pos)
        record.setField(innerAccessor!!.set(inner, fieldValue), pos)
        return record
    }

    companion object {
        private const val serialVersionUID = 1L
    }

    init {
        Preconditions.checkNotNull(typeInfo, "typeInfo must not be null.")
        Preconditions.checkNotNull(innerAccessor, "innerAccessor must not be null.")
        val arity = (typeInfo as TupleTypeInfo<*>).arity
        if (pos in 0 until arity) {
            this.pos = pos
            this.innerAccessor = innerAccessor
            super.internalFieldType = innerAccessor.internalFieldType
        } else {
            throw InvalidFieldReferenceException(
                "Tried to select " + Integer.valueOf(pos)
                    .toString() + ". field on \"" + typeInfo.toString() + "\", which is an invalid index."
            )
        }
    }
}

internal class NullCheckingSimpleTupleFieldAccessor<T : Tuple?, F>(pos: Int, typeInfo: TypeInformation<T>) :
    NullCheckingFieldAccessor<T, F>() {
    private var pos = 0
    override fun get(record: T): F {
        return record!!.getField(pos)
    }

    override fun set(record: T, fieldValue: F): T {
        record!!.setField(fieldValue, pos)
        return record
    }

    companion object {
        private const val serialVersionUID = 1L
    }

    init {
        Preconditions.checkNotNull(typeInfo, "typeInfo must not be null.")
        val arity = (typeInfo as TupleTypeInfo<*>).arity
        if (pos in 0 until arity) {
            this.pos = pos
            super.internalFieldType = typeInfo.getTypeAt<TupleTypeInfo<*>>(pos)
        } else {
            throw InvalidFieldReferenceException(
                "Tried to select " + Integer.valueOf(pos)
                    .toString() + ". field on \"" + typeInfo.toString() + "\", which is an invalid index."
            )
        }
    }
}

internal class NullCheckingArrayFieldAccessor<T, F>(pos: Int, typeInfo: TypeInformation<*>) :
    NullCheckingFieldAccessor<T, F>() {
    private var pos = 0
    override fun get(record: T): F {
        @Suppress("UNCHECKED_CAST")
        return java.lang.reflect.Array.get(record, pos) as F
    }

    override fun set(record: T, fieldValue: F): T {
        java.lang.reflect.Array.set(record, pos, fieldValue)
        return record
    }

    companion object {
        private const val serialVersionUID = 1L
    }

    init {
        if (pos < 0) {
            throw InvalidFieldReferenceException(
                "The " + Integer.valueOf(pos)
                    .toString() + ". field selected on an array, which is an invalid index."
            )
        } else {
            Preconditions.checkNotNull(typeInfo, "typeInfo must not be null.")
            this.pos = pos
            super.internalFieldType = BasicTypeInfo.getInfoFor(typeInfo.typeClass.componentType)
        }
    }
}

internal class NullCheckingSimpleFieldAccessor<T>(typeInfo: TypeInformation<T>?) : NullCheckingFieldAccessor<T, T>() {
    override fun get(record: T): T {
        return record
    }

    override fun set(record: T, fieldValue: T): T {
        return fieldValue
    }

    companion object {
        private const val serialVersionUID = 1L
    }

    init {
        Preconditions.checkNotNull(typeInfo, "typeInfo must not be null.")
        super.internalFieldType = typeInfo
    }
}
